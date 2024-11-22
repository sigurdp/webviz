import logging
from typing import Dict, List, Optional

import httpx
import numpy as np
from fmu.sumo.explorer._utils import Utils as InternalExplorerUtils
from fmu.sumo.explorer.objects import CaseCollection
from pydantic import BaseModel
from sumo.wrapper import SumoClient

from primary import config
from primary.services.sumo_access.sumo_blob_access import (
    get_sas_token_and_blob_store_base_uri_for_case,
)

from webviz_pkg.core_utils.perf_metrics import PerfMetrics


LOGGER = logging.getLogger(__name__)


class RealizationSampleResult(BaseModel):
    realization: int
    sampledValues: list[float]


class _RealizationObjectId(BaseModel):
    realization: int
    objectUuid: str


class _PointSamplingRequestBody(BaseModel):
    sasToken: str
    blobStoreBaseUri: str
    objectIds: List[_RealizationObjectId]
    xCoords: List[float]
    yCoords: List[float]


class _PointSamplingResponseBody(BaseModel):
    sampleResultArr: List[RealizationSampleResult]
    undefLimit: float
    calculationTime_ms: int


# URL of the Go server endpoint
SERVICE_ENDPOINT = f"{config.SURFACE_QUERY_URL}/sample_in_points"


async def batch_sample_surface_in_points_async(
    async_client: httpx.AsyncClient,
    sumo_access_token: str,
    case_uuid: str,
    iteration_name: str,
    surface_name: str,
    surface_attribute: str,
    realizations: Optional[List[int]],
    x_coords: list[float],
    y_coords: list[float],
) -> List[RealizationSampleResult]:

    perf_metrics = PerfMetrics()
    sumo_client = SumoClient(
        env=config.SUMO_ENV, token=sumo_access_token, interactive=False
    )
    perf_metrics.record_lap("create sumo-client")

    # realization_object_ids = await _get_object_uuids_for_surface_realizations(
    #     sumo_client=sumo_client,
    #     case_uuid=case_uuid,
    #     iteration_name=iteration_name,
    #     surface_name=surface_name,
    #     surface_attribute=surface_attribute,
    #     realizations=realizations,
    # )
    # perf_metrics.record_lap("get-obj-ids-using-explorer")

    realization_object_ids_using_es = await get_surface_blob_uuids_es(
        sumo_client=sumo_client,
        case_uuid=case_uuid,
        iteration_name=iteration_name,
        surface_name=surface_name,
        surface_attribute=surface_attribute,
        realizations=realizations,
    )
    perf_metrics.record_lap("get-obj-ids-using-es")

    ## Check that they are the same by sorting on realization and checking length and blob ids
    # realization_object_ids.sort(key=lambda x: x.realization)
    # realization_object_ids_using_es.sort(key=lambda x: x.realization)

    # assert len(realization_object_ids) == len(realization_object_ids_using_es)
    # for i in range(len(realization_object_ids)):
    #     assert (
    #         realization_object_ids[i].realization
    #         == realization_object_ids_using_es[i].realization
    #     )
    #     assert (
    #         realization_object_ids[i].objectUuid
    #         == realization_object_ids_using_es[i].objectUuid
    #     )

    sas_token, blob_store_base_uri = get_sas_token_and_blob_store_base_uri_for_case(
        sumo_access_token, case_uuid
    )

    # LOGGER.debug(f"Token: {sas_token}")

    perf_metrics.record_lap("sas-token")

    request_body = _PointSamplingRequestBody(
        sasToken=sas_token,
        blobStoreBaseUri=blob_store_base_uri,
        objectIds=realization_object_ids_using_es,
        xCoords=x_coords,
        yCoords=y_coords,
    )

    json_request_body = request_body.model_dump()

    LOGGER.info(f"Running async go point sampling for surface: {surface_name}")
    perf_metrics.record_lap("prepare_call")

    response: httpx.Response = await async_client.post(
        url=SERVICE_ENDPOINT, json=json_request_body
    )

    # async with httpx.AsyncClient(timeout=300) as client:
    #     LOGGER.info(f"Running async go point sampling for surface: {surface_name}")

    #     perf_metrics.record_lap("prepare_call")

    #     response: httpx.Response = await client.post(url=SERVICE_ENDPOINT, json=json_request_body)

    perf_metrics.record_lap("main-call")

    json_data: bytes = response.content

    response_body = _PointSamplingResponseBody.model_validate_json(json_data)
    perf_metrics.set_metric("inner-go-call", response_body.calculationTime_ms)
    
    perf_metrics.record_lap("validate-response")

    # Replace values above the undefLimit with np.nan
    for res in response_body.sampleResultArr:
        values_np = np.asarray(res.sampledValues)
        res.sampledValues = np.where(
            (values_np < response_body.undefLimit), values_np, np.nan
        ).tolist()
    perf_metrics.record_lap("post-process")
    LOGGER.debug(
        f"------------------ batch_sample_surface_in_points_async() took: {perf_metrics.to_string_s()}"
    )

    return response_body.sampleResultArr


async def _get_object_uuids_for_surface_realizations(
    sumo_client: SumoClient,
    case_uuid: str,
    iteration_name: str,
    surface_name: str,
    surface_attribute: str,
    realizations: Optional[List[int]],
) -> List[_RealizationObjectId]:

    case_collection = CaseCollection(sumo_client).filter(uuid=case_uuid)
    case = await case_collection.getitem_async(0)

    # What about time here??
    surface_collection = case.surfaces.filter(
        iteration=iteration_name,
        name=surface_name,
        tagname=surface_attribute,
        realization=realizations,
    )

    # Is this the right way to get hold of the object uuids?
    internal_explorer_utils = InternalExplorerUtils(sumo_client)
    # pylint: disable=protected-access
    object_meta_list: List[Dict] = await internal_explorer_utils.get_objects_async(
        500, surface_collection._query, ["_id", "fmu.realization.id"]
    )

    ret_list: List[_RealizationObjectId] = []
    for obj_meta in object_meta_list:
        ret_list.append(
            _RealizationObjectId(
                realization=obj_meta["_source"]["fmu"]["realization"]["id"],
                objectUuid=obj_meta["_id"],
            )
        )

    return ret_list


async def get_surface_blob_uuids_es(
    sumo_client: SumoClient,
    case_uuid: str,
    iteration_name: str,
    surface_name: str,
    surface_attribute: str,
    realizations: Optional[List[int]],
) -> List[_RealizationObjectId]:
    """Get the blob ids and realization ids for realization surfaces"""

    query: Dict = {
        "bool": {
            "should": [
                {
                    "bool": {
                        "must": [
                            {"term": {"_sumo.parent_object.keyword": case_uuid}},
                            {"term": {"class.keyword": "surface"}},
                            {"term": {"fmu.iteration.name.keyword": iteration_name}},
                            {"term": {"fmu.context.stage": "realization"}},
                            {"term": {"data.name.keyword": surface_name}},
                            {"term": {"data.tagname.keyword": surface_attribute}},
                            {"terms": {"fmu.realization.id": realizations}},
                        ]
                    }
                },
            ],
            "minimum_should_match": 1,
        },
    }
    aggs = {
        "key_combinations": {
            "composite": {
                "size": 65535,
                "sources": [
                    {"k_reals": {"terms": {"field": "fmu.realization.id"}}},
                    {"k_uuids": {"terms": {"field": "_sumo.blob_name.keyword"}}},
                ],
            }
        }
    }
    payload = {
        "query": query,
        "aggs": aggs,
        "size": 0,
    }
    response = await sumo_client.post_async("/search", json=payload)

    result = response.json()

    aggs = result["aggregations"]["key_combinations"]["buckets"]
    realization_object_ids: List[_RealizationObjectId] = []
    for agg in aggs:
        realization = agg["key"]["k_reals"]
        blob_uuid = agg["key"]["k_uuids"]
        realization_object_ids.append(
            _RealizationObjectId(realization=realization, objectUuid=blob_uuid)
        )
    return realization_object_ids
