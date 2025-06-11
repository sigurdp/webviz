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
from .surface_query_service import _RealizationObjectId

LOGGER = logging.getLogger(__name__)


class RealizationSampleResult(BaseModel):
    realization: int
    sampledValues: list[float]


class _SampleInPointsTaskInput(BaseModel):
    spike_userId: str
    spike_resultCacheKey: str
    sasToken: str
    blobStoreBaseUri: str
    objectIds: List[_RealizationObjectId]
    xCoords: List[float]
    yCoords: List[float]


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

    params_for_hash = {
        "case_uuid": case_uuid,
        "iteration_name": iteration_name,
        "surface_name": surface_name,
        "surface_attribute": surface_attribute,
        "realizations": sorted(realizations),  # Sort for consistent hashing
        "sample_points": {
            "x_points": x_coords,
            "y_points": y_coords
        }
    }
    params_json = json.dumps(params_for_hash, sort_keys=True, separators=(',', ':'))
    params_hash = hashlib.sha256(params_json.encode('utf-8')).hexdigest()

    temp_user_store = get_temp_user_store_for_user(authenticated_user)

    store_key = f"task_based_sample_surface_in_points__{params_hash}"
    existing_result = await temp_user_store.get_pydantic_model(
        model_class=_SampleInPointsTaskResult,
        key=store_key,
        format="msgpack"
    )
    # if existing_result is not None:
    #     LOGGER.debug(f"Found existing result key: {store_key}")
    #     ret_array = _transform_return_array(response_body=existing_result)
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

    request_body = _SampleInPointsTaskInput(
        spike_userId=user_id,
        spike_resultCacheKey=store_key,
        sasToken=sas_token,
        blobStoreBaseUri=blob_store_base_uri,
        objectIds=realization_object_ids,
        xCoords=x_coords,
        yCoords=y_coords,
    )

    LOGGER.info(f"Enqueuing go task for point sampling on surface: {surface_name}")
    response: httpx.Response = await HTTPX_ASYNC_CLIENT_WRAPPER.client.post(url=f"{config.SURFACE_QUERY_URL}/enqueue_task/sample_in_points", json=request_body.model_dump())
    task_status = _TaskStatusResponse.model_validate_json(response.content) 
    perf_metrics.record_lap("enqueue-go")

    task_id = task_status.taskId
    timeout = 100.0  # seconds
    poll_interval = 0.5  # seconds
    waited = 0
    done = False

    while not done and waited < timeout:
        response: httpx.Response = await HTTPX_ASYNC_CLIENT_WRAPPER.client.get(url=f"{config.SURFACE_QUERY_URL}/task_status/{task_id}")
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
        model_class=_SampleInPointsTaskResult,
        key=store_key,
        format="msgpack"
    )
    perf_metrics.record_lap("fetch-result")

    if new_result is None:
        LOGGER.error(f"Task {task_id} completed but no result was found in temp user store for key: {store_key}")
        raise RuntimeError(f"Task {task_id} completed but no result was found in temp user store for key: {store_key}")

    ret_array = _transform_return_array(response_body=new_result)
    perf_metrics.record_lap("parse-result")

    LOGGER.debug(f"task_based_sample_surface_in_points_async() took: {perf_metrics.to_string()}")

    return ret_array


def _transform_return_array(response_body = _SampleInPointsTaskResult) -> List[RealizationSampleResult]:
    # Replace values above the undefLimit with np.nan
    for res in response_body.sampleResultArr:
        values_np = np.asarray(res.sampledValues)
        res.sampledValues = np.where((values_np < response_body.undefLimit), values_np, np.nan).tolist()

    return response_body.sampleResultArr


