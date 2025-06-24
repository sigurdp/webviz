import logging
from typing import Any, List, Optional, Literal

import httpx
import numpy as np
from pydantic import BaseModel
from webviz_pkg.core_utils.perf_metrics import PerfMetrics
from fmu.sumo.explorer.objects import SearchContext
from dataclasses import dataclass

from primary import config
from primary.services.sumo_access.sumo_blob_access import get_sas_token_and_blob_base_uri_for_case_async
from primary.services.utils.httpx_async_client_wrapper import HTTPX_ASYNC_CLIENT_WRAPPER
from primary.services.sumo_access.sumo_client_factory import create_sumo_client
from primary.services.utils.task_meta_tracker import get_task_meta_tracker_for_user
import asyncio

import hashlib
import json
from primary.services.utils.authenticated_user import AuthenticatedUser
from primary.services.utils.temp_user_store import get_temp_user_store_for_user
from pydantic import BaseModel

from .surface_query_service import RealizationSampleResult

from . import task_schemas


LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True, kw_only=True)
class TaskStatus:
    task_id: str
    status: Literal["in_progress", "failure", "success"]
    progress_msg: str | None = None
    error_msg: str | None = None




# =========================================================
# =========================================================
async def tb_hybrid_sample_in_points_async(
    authenticated_user: AuthenticatedUser,
    case_uuid: str,
    iteration_name: str,
    surface_name: str,
    surface_attribute: str,
    realizations: Optional[List[int]],
    x_coords: list[float],
    y_coords: list[float],
) -> TaskStatus | List[RealizationSampleResult]:

    param_hash = _compute_parameter_hash(
        case_uuid=case_uuid,
        iteration_name=iteration_name,
        surface_name=surface_name,
        surface_attribute=surface_attribute,
        realizations=realizations,
        x_coords=x_coords,
        y_coords=y_coords,
    )
    store_key = _build_store_key_for_temp_user_store(param_hash)

    temp_user_store = get_temp_user_store_for_user(authenticated_user)
    existing_result = await temp_user_store.get_pydantic_model(model_class=task_schemas.SampleInPointsTaskResult, key=store_key, format="msgpack")
    if existing_result:
        ret_array = _transform_return_array(task_result=existing_result)
        return ret_array

    # This is basically de-duplication
    # Checking whether a task with the same parameters has already been started AND is still pending or running
    tracker = get_task_meta_tracker_for_user(authenticated_user)
    existing_task_id = await tracker.get_task_id_by_payload_hash_async(payload_hash=param_hash)
    if existing_task_id:
        existing_task_status = await _fetch_and_translate_task_status_or_none_async(existing_task_id)
        
        if existing_task_status and existing_task_status.status == "in_progress":
            return existing_task_status

        # Delete the payload mapping for this task, so that it can be retried
        await tracker.delete_payload_hash_to_task_mapping_async(payload_hash=param_hash)

        # If it is in a failed state, we report the error status and require a new invocation
        # For other outcomes (task not found or task completed) we will proceed with a new task launch
        if existing_task_status and existing_task_status.status == "failure":
            return existing_task_status

    # Launch a new task
    task_id = await tb_start_sample_in_points_async(
        authenticated_user=authenticated_user,
        case_uuid=case_uuid,
        iteration_name=iteration_name,
        surface_name=surface_name,
        surface_attribute=surface_attribute,
        realizations=realizations,
        x_coords=x_coords,
        y_coords=y_coords,
    )

    await tracker.set_payload_hash_to_task_mapping_async(payload_hash=param_hash, task_id=task_id)

    return TaskStatus(task_id=task_id, status="in_progress", progress_msg="Task submitted")




# =========================================================
# =========================================================
async def tb_start_sample_in_points_async(
    authenticated_user: AuthenticatedUser,
    case_uuid: str,
    iteration_name: str,
    surface_name: str,
    surface_attribute: str,
    realizations: Optional[List[int]],
    x_coords: list[float],
    y_coords: list[float],
) -> str:
    perf_metrics = PerfMetrics()

    user_id = authenticated_user.get_user_id()
    sumo_access_token = authenticated_user.get_sumo_access_token()

    param_hash = _compute_parameter_hash(
        case_uuid=case_uuid,
        iteration_name=iteration_name,
        surface_name=surface_name,
        surface_attribute=surface_attribute,
        realizations=realizations,
        x_coords=x_coords,
        y_coords=y_coords,
    )
    store_key = _build_store_key_for_temp_user_store(param_hash)

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

    task_input = task_schemas.SampleInPointsTaskInput(
        userId=user_id,
        sasToken=sas_token,
        blobStoreBaseUri=blob_store_base_uri,
        realizationSurfaceObjects=realization_object_ids,
        xCoords=x_coords,
        yCoords=y_coords,
        targetStoreKey=store_key,
    )

    LOGGER.info(f"Enqueuing go task for point sampling on surface: {surface_name}")
    response: httpx.Response = await HTTPX_ASYNC_CLIENT_WRAPPER.client.post(
        url=f"{config.SURFACE_QUERY_URL}/enqueue_task/sample_in_points", json=task_input.model_dump()
    )
    response.raise_for_status()
    task_status = task_schemas.TaskStatusResponse.model_validate_json(response.content)
    perf_metrics.record_lap("enqueue-go")

    task_tracker = get_task_meta_tracker_for_user(authenticated_user)
    await task_tracker.register_task_async(
        task_system="asynq", 
        task_id=task_status.taskId,
        expected_store_key=store_key
    )

    return task_status.taskId


# =========================================================
# =========================================================
async def tb_poll_sample_in_points_async(
    authenticated_user: AuthenticatedUser,
    task_id: str
) -> TaskStatus | List[RealizationSampleResult]:
    tracker = get_task_meta_tracker_for_user(authenticated_user)
    task_meta = await tracker.get_task_meta_async(task_id=task_id)
    if task_meta is None:
        raise ValueError("Task not found")

    if task_meta.expected_store_key is None:
        raise ValueError("Task has no expected store key")

    temp_user_store = get_temp_user_store_for_user(authenticated_user)

    task_result = await temp_user_store.get_pydantic_model(model_class=task_schemas.SampleInPointsTaskResult, key=task_meta.expected_store_key, format="msgpack")
    if task_result:
        ret_array = _transform_return_array(task_result=task_result)
        return ret_array

    task_status: TaskStatus = await _fetch_and_translate_task_status_async(task_id=task_id)
    return task_status


async def _fetch_and_translate_task_status_async(task_id: str) -> TaskStatus:
    response: httpx.Response = await HTTPX_ASYNC_CLIENT_WRAPPER.client.get(url=f"{config.SURFACE_QUERY_URL}/task_status/{task_id}")
    response.raise_for_status()
    task_status_response = task_schemas.TaskStatusResponse.model_validate_json(response.content)

    if task_status_response.status == "pending" or task_status_response.status == "running":
        return TaskStatus(task_id=task_id, status="in_progress", progress_msg=task_status_response.status)
    
    elif task_status_response.status == "failure":
        return TaskStatus(task_id=task_id, status="failure", error_msg=task_status_response.errorMsg)
        
    elif task_status_response.status == "success":
        return TaskStatus(task_id=task_id, status="success")

    raise ValueError(f"Unexpected task status: {task_status_response.status}")


async def _fetch_and_translate_task_status_or_none_async(task_id: str) -> TaskStatus | None:
    try:
        task_status: TaskStatus = await _fetch_and_translate_task_status_async(task_id=task_id)
        return task_status
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404:
            return None
        LOGGER.error(f"HTTP error while querying task status for {task_id}: {e}")
        raise e
    except Exception as e:
        LOGGER.error(f"Unexpected error while querying task status for {task_id}: {e}")
        raise e


async def tb_start_and_poll_sample_in_points_async(
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

    param_hash = _compute_parameter_hash(
        case_uuid=case_uuid,
        iteration_name=iteration_name,
        surface_name=surface_name,
        surface_attribute=surface_attribute,
        realizations=realizations,
        x_coords=x_coords,
        y_coords=y_coords,
    )
    store_key = _build_store_key_for_temp_user_store(param_hash)

    temp_user_store = get_temp_user_store_for_user(authenticated_user)
    existing_result = await temp_user_store.get_pydantic_model(
        model_class=task_schemas.SampleInPointsTaskResult, key=store_key, format="msgpack"
    )
    # if existing_result is not None:
    #     LOGGER.debug(f"Found existing result key: {store_key}")
    #     ret_array = _transform_return_array(task_result=existing_result)
    #     perf_metrics.record_lap("fetch-existing-result")
    #     LOGGER.debug(f"task_based_sample_surface_in_points_async() took: {perf_metrics.to_string()}")
    #     return ret_array

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

    request_body = task_schemas.SampleInPointsTaskInput(
        userId=user_id,
        sasToken=sas_token,
        blobStoreBaseUri=blob_store_base_uri,
        realizationSurfaceObjects=realization_object_ids,
        xCoords=x_coords,
        yCoords=y_coords,
        targetStoreKey=store_key,
    )

    LOGGER.info(f"Enqueuing go task for point sampling on surface: {surface_name}")
    response: httpx.Response = await HTTPX_ASYNC_CLIENT_WRAPPER.client.post(
        url=f"{config.SURFACE_QUERY_URL}/enqueue_task/sample_in_points", json=request_body.model_dump()
    )
    response.raise_for_status()
    task_status_response = task_schemas.TaskStatusResponse.model_validate_json(response.content)
    perf_metrics.record_lap("enqueue-go")

    task_id = task_status_response.taskId
    timeout: float = 100.0  # seconds
    poll_interval: float = 0.5  # seconds
    waited: float = 0
    done = False

    while not done and waited < timeout:
        task_status: TaskStatus = await _fetch_and_translate_task_status_async(task_id=task_id)
        status_str = task_status.status
        if status_str in ["success", "failure"]:
            done = True
        else:
            await asyncio.sleep(poll_interval)
            waited += poll_interval
            progress_msg = task_status.progress_msg
            LOGGER.debug(f"waiting for task {task_id} to complete... {waited=:.1f} {status_str=} {progress_msg=}")

    perf_metrics.record_lap("poll-status")

    if task_status.status == "in_progress":
        LOGGER.error(f"Task {task_id} did not complete within the timeout period of {timeout} seconds.")
        raise TimeoutError(f"Task {task_id} did not complete within the timeout period of {timeout} seconds.")

    if task_status.status == "failure":
        LOGGER.error(f"Task {task_id} failed: {task_status.error_msg=}")
        raise RuntimeError(f"Task {task_id} failed: {task_status.error_msg}")

    new_result = await temp_user_store.get_pydantic_model(
        model_class=task_schemas.SampleInPointsTaskResult, key=store_key, format="msgpack"
    )
    perf_metrics.record_lap("fetch-result")

    if new_result is None:
        LOGGER.error(f"Task {task_id} completed but no result was found in temp user store for key: {store_key}")
        raise RuntimeError(f"Task {task_id} completed but no result was found in temp user store for key: {store_key}")

    ret_array = _transform_return_array(task_result=new_result)
    perf_metrics.record_lap("parse-result")

    LOGGER.debug(f"task_based_sample_surface_in_points_async() took: {perf_metrics.to_string()}")

    return ret_array


def _build_store_key_for_temp_user_store(param_hash: str) -> str:
    store_key = f"task_based_sample_surface_in_points__{param_hash}"
    return store_key


def _compute_parameter_hash(
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
    return _compute_payload_hash(params_for_hash)


def _compute_payload_hash(payload: Any) -> str:
    payload_json = json.dumps(payload, sort_keys=True)
    return hashlib.sha256(payload_json.encode()).hexdigest()


def _transform_return_array(task_result: task_schemas.SampleInPointsTaskResult) -> List[RealizationSampleResult]:
    ret_arr: List[RealizationSampleResult] = []
    
    # Replace values above the undefLimit with np.nan
    for realization_samples in task_result.realizationSamples:
        values_np = np.asarray(realization_samples.sampledValues)
        values_with_nan = np.where((values_np < task_result.undefLimit), values_np, np.nan).tolist()
        ret_arr.append(RealizationSampleResult(realization=realization_samples.realization, sampledValues=values_with_nan))

    return ret_arr 


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

    task_input = task_schemas.BatchSamplePointSetsTaskInput(
        userId=user_id,
        sasToken=sas_token,
        blobStoreBaseUri=blob_store_base_uri,
        realizationSurfaceObjects=realization_object_ids,
        pointSets=[],
    )

    for ps in point_sets:
        param_hash = _compute_parameter_hash(
            case_uuid=case_uuid,
            iteration_name=iteration_name,
            surface_name=surface_name,
            surface_attribute=surface_attribute,
            realizations=realizations,
            x_coords=ps.x_coords,
            y_coords=ps.y_coords,
        )
        store_key = _build_store_key_for_temp_user_store(param_hash)

        task_input.pointSets.append(
            task_schemas.NamedPointSet(name=ps.name, xCoords=ps.x_coords, yCoords=ps.y_coords, targetStoreKey=store_key)
        )

    perf_metrics.record_lap("build-task-input")

    LOGGER.info(
        f"Enqueuing go task for PRECOMPUTE point sampling on surface: {surface_name}, num point sets: {len(task_input.pointSets)}"
    )
    response: httpx.Response = await HTTPX_ASYNC_CLIENT_WRAPPER.client.post(
        url=f"{config.SURFACE_QUERY_URL}/enqueue_task/batch_sample_point_sets", json=task_input.model_dump()
    )
    response.raise_for_status()
    task_status = task_schemas.TaskStatusResponse.model_validate_json(response.content)
    perf_metrics.record_lap("enqueue-go")

    LOGGER.debug(f"start_precompute_sample_surface_in_point_sets_async() took: {perf_metrics.to_string()}")

    return task_status.taskId


async def get_status_of_precompute_sample_surface_in_point_sets_async(task_id: str) -> TaskStatus:
    return await _fetch_and_translate_task_status_async(task_id=task_id)


async def _get_object_uuids_for_surface_realizations_async(
    sumo_access_token: str,
    case_uuid: str,
    iteration_name: str,
    surface_name: str,
    surface_attribute: str,
    realizations: Optional[List[int]],
) -> List[task_schemas.RealizationSurfaceObject]:
    sumo_client = create_sumo_client(sumo_access_token)

    # What about time here??
    search_context = SearchContext(sumo_client).surfaces.filter(
        uuid=case_uuid,
        iteration=iteration_name,
        name=surface_name,
        tagname=surface_attribute,
        realization=realizations if realizations is not None else True,
    )

    ret_list: List[task_schemas.RealizationSurfaceObject] = []

    # Getting just the object uuids seems easy, but we want them paired with realization numbers
    # object_uuids = await search_context.uuids_async

    # For the time being (as of end Feb 2025), this loop seems to be the fastest way to get the (uuids, rid) pairs using Sumo explorer.
    # Alternatively we could try and formulate a custom search
    async for surf in search_context:
        obj_uuid = surf.uuid
        rid = surf.metadata["fmu"]["realization"]["id"]
        ret_list.append(task_schemas.RealizationSurfaceObject(realization=rid, objectUuid=obj_uuid))

    return ret_list
