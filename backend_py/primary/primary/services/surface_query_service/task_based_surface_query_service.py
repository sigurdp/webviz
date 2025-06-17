import logging
from typing import List, Optional, Literal

import httpx
import numpy as np
from pydantic import BaseModel
from webviz_pkg.core_utils.perf_metrics import PerfMetrics

from primary import config
from primary.services.sumo_access.sumo_blob_access import get_sas_token_and_blob_base_uri_for_case_async
from primary.services.utils.httpx_async_client_wrapper import HTTPX_ASYNC_CLIENT_WRAPPER
import asyncio

import hashlib
import json
from primary.services.utils.authenticated_user import AuthenticatedUser
from primary.services.utils.temp_user_store import get_temp_user_store_for_user
from pydantic import BaseModel

from .surface_query_service import _get_object_uuids_for_surface_realizations_async
from .surface_query_service import RealizationSampleResult, _RealizationObjectId

LOGGER = logging.getLogger(__name__)


# class RealizationSampleResult(BaseModel):
#     realization: int
#     sampledValues: list[float]


# class _RealizationSurfaceObject(_RealizationObjectId):
#     realization: int
#     objectUuid: str


class _PointSet(BaseModel):
    name: str
    xCoords: List[float]
    yCoords: List[float]
    targetStoreKey: str


class _SampleInPointsTaskInput(BaseModel):
    userId: str
    sasToken: str
    blobStoreBaseUri: str
    realizationSurfaceObjects: List[_RealizationObjectId]
    pointSets: List[_PointSet]


class _SampleInPointsTaskResult(BaseModel):
    sampleResultArr: List[RealizationSampleResult]
    undefLimit: float


class _TaskStatusResponse(BaseModel):
    taskId: str
    status: Literal["pending", "running", "succeeded", "failed"]
    result: dict | None = None
    errorMsg: str | None = None


async def task_based_sample_surface_in_points_async(
    authenticated_user: AuthenticatedUser,
    case_uuid: str,
    iteration_name: str,
    surface_name: str,
    surface_attribute: str,
    realizations: Optional[List[int]],
    x_coords: list[float],
    y_coords: list[float],
) -> List[RealizationSampleResult]:
    perf_metrics = PerfMetrics()

    user_id = authenticated_user.get_user_id()
    sumo_access_token = authenticated_user.get_sumo_access_token()

    temp_user_store = get_temp_user_store_for_user(authenticated_user)

    store_key = _build_key_for_temp_user_store(
        case_uuid=case_uuid,
        iteration_name=iteration_name,
        surface_name=surface_name,
        surface_attribute=surface_attribute,
        realizations=realizations,
        x_coords=x_coords,
        y_coords=y_coords,
    )
    existing_result = await temp_user_store.get_pydantic_model(
        model_class=_SampleInPointsTaskResult, key=store_key, format="msgpack"
    )
    if existing_result is not None:
        LOGGER.debug(f"Found existing result key: {store_key}")
        ret_array = _transform_return_array(response_body=existing_result)
        perf_metrics.record_lap("fetch-existing-result")
        LOGGER.debug(f"task_based_sample_surface_in_points_async() took: {perf_metrics.to_string()}")
        return ret_array

    LOGGER.debug(f"No cached point sampling result found for key: {store_key}, proceeding with new request...")

    realization_object_ids = await _get_object_uuids_for_surface_realizations_async(
        sumo_access_token=sumo_access_token,
        case_uuid=case_uuid,
        iteration_name=iteration_name,
        surface_name=surface_name,
        surface_attribute=surface_attribute,
        realizations=realizations,
    )
    perf_metrics.record_lap("obj-uuids")

    sas_token, blob_store_base_uri = await get_sas_token_and_blob_base_uri_for_case_async(sumo_access_token, case_uuid)
    perf_metrics.record_lap("sas-token")

    pointSets = []
    pointSets.append(_PointSet(name="DummyName", xCoords=x_coords, yCoords=y_coords, targetStoreKey=store_key))

    # for i in range(100):
    #     pointSets.append(_PointSet(name=f"DummyName{i}", xCoords=x_coords, yCoords=y_coords, targetStoreKey=store_key+ f"_{i}"))

    request_body = _SampleInPointsTaskInput(
        userId=user_id,
        sasToken=sas_token,
        blobStoreBaseUri=blob_store_base_uri,
        realizationSurfaceObjects=realization_object_ids,
        pointSets=pointSets,
    )

    LOGGER.info(f"Enqueuing go task for point sampling on surface: {surface_name}")
    response: httpx.Response = await HTTPX_ASYNC_CLIENT_WRAPPER.client.post(
        url=f"{config.SURFACE_QUERY_URL}/enqueue_task/sample_in_points", json=request_body.model_dump()
    )
    response.raise_for_status()
    task_status = _TaskStatusResponse.model_validate_json(response.content)
    perf_metrics.record_lap("enqueue-go")

    task_id = task_status.taskId
    timeout: float = 100.0  # seconds
    poll_interval: float = 0.5  # seconds
    waited: float = 0
    done = False

    while not done and waited < timeout:
        response = await HTTPX_ASYNC_CLIENT_WRAPPER.client.get(url=f"{config.SURFACE_QUERY_URL}/task_status/{task_id}")
        response.raise_for_status()
        task_status = _TaskStatusResponse.model_validate_json(response.content)

        status_string = task_status.status
        if status_string in ["succeeded", "failed"]:
            done = True
        else:
            await asyncio.sleep(poll_interval)
            waited += poll_interval
            LOGGER.debug(f"waiting for task {task_id} to complete... {waited=:.1f} {status_string=}")

    perf_metrics.record_lap("poll-status")

    if task_status.status in ["pending", "running"]:
        LOGGER.error(f"Task {task_id} did not complete within the timeout period of {timeout} seconds.")
        raise TimeoutError(f"Task {task_id} did not complete within the timeout period of {timeout} seconds.")

    if task_status.status == "error":
        LOGGER.error(f"Task {task_id} encountered an error: {task_status.errorMsg=}")
        raise RuntimeError(f"Task {task_id} encountered an error: {task_status.errorMsg}")

    new_result = await temp_user_store.get_pydantic_model(
        model_class=_SampleInPointsTaskResult, key=store_key, format="msgpack"
    )
    perf_metrics.record_lap("fetch-result")

    if new_result is None:
        LOGGER.error(f"Task {task_id} completed but no result was found in temp user store for key: {store_key}")
        raise RuntimeError(f"Task {task_id} completed but no result was found in temp user store for key: {store_key}")

    ret_array = _transform_return_array(response_body=new_result)
    perf_metrics.record_lap("parse-result")

    LOGGER.debug(f"task_based_sample_surface_in_points_async() took: {perf_metrics.to_string()}")

    return ret_array


def _build_key_for_temp_user_store(
    case_uuid: str,
    iteration_name: str,
    surface_name: str,
    surface_attribute: str,
    realizations: List[int] | None,
    x_coords: list[float],
    y_coords: list[float],
) -> str:

    params_for_hash = {
        "case_uuid": case_uuid,
        "iteration_name": iteration_name,
        "surface_name": surface_name,
        "surface_attribute": surface_attribute,
        "realizations": sorted(realizations) if realizations else None,  # Sort for consistent hashing
        "sample_points": {"x_points": x_coords, "y_points": y_coords},
    }
    params_json = json.dumps(params_for_hash, sort_keys=True, separators=(",", ":"))
    params_hash = hashlib.sha256(params_json.encode("utf-8")).hexdigest()

    store_key = f"task_based_sample_surface_in_points__{params_hash}"

    return store_key


def _transform_return_array(response_body: _SampleInPointsTaskResult) -> List[RealizationSampleResult]:
    # Replace values above the undefLimit with np.nan
    for res in response_body.sampleResultArr:
        values_np = np.asarray(res.sampledValues)
        res.sampledValues = np.where((values_np < response_body.undefLimit), values_np, np.nan).tolist()

    return response_body.sampleResultArr


class NamedPointSet(BaseModel):
    name: str
    x_coords: list[float]
    y_coords: list[float]


async def start_precompute_sample_surface_in_point_sets_async(
    authenticated_user: AuthenticatedUser,
    case_uuid: str,
    iteration_name: str,
    surface_name: str,
    surface_attribute: str,
    realizations: Optional[List[int]],
    point_sets: List[NamedPointSet],
) -> str:

    perf_metrics = PerfMetrics()

    user_id = authenticated_user.get_user_id()
    sumo_access_token = authenticated_user.get_sumo_access_token()

    realization_object_ids = await _get_object_uuids_for_surface_realizations_async(
        sumo_access_token=sumo_access_token,
        case_uuid=case_uuid,
        iteration_name=iteration_name,
        surface_name=surface_name,
        surface_attribute=surface_attribute,
        realizations=realizations,
    )
    perf_metrics.record_lap("obj-uuids")

    sas_token, blob_store_base_uri = await get_sas_token_and_blob_base_uri_for_case_async(sumo_access_token, case_uuid)
    perf_metrics.record_lap("sas-token")

    task_input = _SampleInPointsTaskInput(
        userId=user_id,
        sasToken=sas_token,
        blobStoreBaseUri=blob_store_base_uri,
        realizationSurfaceObjects=realization_object_ids,
        pointSets=[],
    )

    for ps in point_sets:
        store_key = _build_key_for_temp_user_store(
            case_uuid=case_uuid,
            iteration_name=iteration_name,
            surface_name=surface_name,
            surface_attribute=surface_attribute,
            realizations=realizations,
            x_coords=ps.x_coords,
            y_coords=ps.y_coords,
        )

        task_input.pointSets.append(
            _PointSet(name=ps.name, xCoords=ps.x_coords, yCoords=ps.y_coords, targetStoreKey=store_key)
        )

    perf_metrics.record_lap("build-task-input")

    LOGGER.info(
        f"Enqueuing go task for PRECOMPUTE point sampling on surface: {surface_name}, num point sets: {len(task_input.pointSets)}"
    )
    response: httpx.Response = await HTTPX_ASYNC_CLIENT_WRAPPER.client.post(
        url=f"{config.SURFACE_QUERY_URL}/enqueue_task/sample_in_points", json=task_input.model_dump()
    )
    response.raise_for_status()
    task_status = _TaskStatusResponse.model_validate_json(response.content)
    perf_metrics.record_lap("enqueue-go")

    LOGGER.debug(f"start_precompute_sample_surface_in_point_sets_async() took: {perf_metrics.to_string()}")

    return task_status.taskId


class TaskStatus(BaseModel):
    status: Literal["in_progress", "failure", "success"]
    error_msg: str | None = None


async def get_status_of_precompute_sample_surface_in_point_sets_async(task_id: str) -> TaskStatus:
    response: httpx.Response = await HTTPX_ASYNC_CLIENT_WRAPPER.client.get(
        url=f"{config.SURFACE_QUERY_URL}/task_status/{task_id}"
    )
    response.raise_for_status()
    task_status_response = _TaskStatusResponse.model_validate_json(response.content)

    if task_status_response.status == "pending" or task_status_response.status == "running":
        return TaskStatus(status="in_progress")
    elif task_status_response.status == "error":
        return TaskStatus(status="error", errorMsg=task_status_response.errorMsg)
    elif task_status_response.status == "succeeded":
        return TaskStatus(status="success")

    raise ValueError(f"Unexpected task status: {task_status_response.status}")
