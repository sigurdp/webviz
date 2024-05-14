import asyncio
import logging
from typing import Annotated

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query, Path


import json
import io
import fsspec
import adlfs
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as ds
from webviz_pkg.core_utils.perf_timer import PerfTimer
from fastapi.responses import HTMLResponse
from primary.services.sumo_access.sumo_blob_access import get_sas_token_and_blob_store_base_uri_for_case
from sumo.wrapper import SumoClient
from primary import config
import pyarrow.feather as pf



from webviz_pkg.core_utils.background_tasks import run_in_background_task

from primary.auth.auth_helper import AuthenticatedUser, AuthHelper
from primary.services.user_session_manager.user_session_manager import UserSessionManager
from primary.services.user_session_manager.user_session_manager import UserComponent
from primary.services.user_session_manager.user_session_manager import _USER_SESSION_DEFS
from primary.services.user_session_manager._radix_helpers import RadixResourceRequests, RadixJobApi
from primary.services.user_session_manager._user_session_directory import UserSessionDirectory
from primary.services.user_grid3d_service.user_grid3d_service import UserGrid3dService, IJKIndexFilter

LOGGER = logging.getLogger(__name__)


router = APIRouter()


@router.get("/usersession/{user_component}/call")
async def usersession_call(
    authenticated_user: Annotated[AuthenticatedUser, Depends(AuthHelper.get_authenticated_user)],
    user_component: Annotated[UserComponent, Path(description="User session component")],
    instance_str: Annotated[str, Query(description="Instance string")] = "myInst",
) -> str:
    LOGGER.debug(f"usersession_call() {user_component=}, {instance_str=}")
    LOGGER.debug(f"usersession_call() {authenticated_user.get_user_id()=}, {authenticated_user.get_username()=}")

    manager = UserSessionManager(authenticated_user.get_user_id(), authenticated_user.get_username())
    session_base_url = await manager.get_or_create_session_async(user_component, instance_str)
    if session_base_url is None:
        LOGGER.error("Failed to get user session URL")
        raise HTTPException(status_code=500, detail="Failed to get user session URL")

    endpoint = f"{session_base_url}/dowork?duration=5"

    LOGGER.debug("======================")
    LOGGER.debug(f"{session_base_url=}")
    LOGGER.debug(f"{endpoint=}")
    LOGGER.debug("======================")

    LOGGER.debug(f"before call to: {endpoint=}")

    async with httpx.AsyncClient(timeout=30) as client:
        response = await client.get(endpoint)
        response.raise_for_status()

    LOGGER.debug(f"after call to: {endpoint=}")

    resp_text = response.text
    LOGGER.debug(f"{type(resp_text)=}")
    LOGGER.debug(f"{resp_text=}")

    return resp_text


@router.get("/usersession/{user_component}/radixlist")
async def usersession_radixlist(user_component: UserComponent) -> list:
    LOGGER.debug(f"usersession_radixlist() {user_component=}")

    session_def = _USER_SESSION_DEFS[user_component]
    radix_job_api = RadixJobApi(session_def.job_component_name, session_def.port)

    job_list = await radix_job_api.get_all_jobs()

    LOGGER.debug("======================")
    LOGGER.debug(job_list)
    LOGGER.debug("======================")

    return job_list


@router.get("/usersession/{user_component}/radixcreate")
async def usersession_radixcreate(user_component: UserComponent) -> str:
    LOGGER.debug(f"usersession_radixcreate() {user_component=}")

    session_def = _USER_SESSION_DEFS[user_component]
    radix_job_api = RadixJobApi(session_def.job_component_name, session_def.port)
    resource_req = RadixResourceRequests(cpu="50m", memory="100Mi")
    new_radix_job_name = await radix_job_api.create_new_job(
        resource_req=resource_req, job_id="dummyJobId", payload_dict=None
    )
    LOGGER.debug(f"Created new job: {new_radix_job_name=}")
    if new_radix_job_name is None:
        return "Failed to create new job"

    LOGGER.debug(f"Polling job until receiving running status: {new_radix_job_name=}")
    max_state_calls = 20
    for _i in range(max_state_calls):
        radix_job_state = await radix_job_api.get_job_state(new_radix_job_name)
        session_status = radix_job_state.status if radix_job_state else "N/A"
        LOGGER.debug(f"Status: {session_status=}")
        await asyncio.sleep(0.1)

    return str(radix_job_state)


@router.get("/usersession/{user_component}/radixdelete")
async def usersession_radixdelete(user_component: UserComponent) -> str:
    LOGGER.debug(f"usersession_radixdelete() {user_component=}")

    session_def = _USER_SESSION_DEFS[user_component]
    radix_job_api = RadixJobApi(session_def.job_component_name, session_def.port)

    await radix_job_api.delete_all_jobs()

    return "Delete done"


@router.get("/usersession/dirlist")
async def usersession_dirlist(
    authenticated_user: Annotated[AuthenticatedUser, Depends(AuthHelper.get_authenticated_user)],
    user_component: UserComponent | None = None,
) -> list:
    LOGGER.debug(f"usersession_dirlist() {user_component=}")

    job_component_name: str | None = None
    if user_component is not None:
        job_component_name = _USER_SESSION_DEFS[user_component].job_component_name

    session_dir = UserSessionDirectory(authenticated_user.get_user_id())
    session_info_arr = session_dir.get_session_info_arr(job_component_name)

    LOGGER.debug("======================")
    for session_info in session_info_arr:
        LOGGER.debug(f"{session_info=}")
    LOGGER.debug("======================")

    return session_info_arr


@router.get("/usersession/dirdel")
async def usersession_dirdel(
    authenticated_user: Annotated[AuthenticatedUser, Depends(AuthHelper.get_authenticated_user)],
    user_component: UserComponent | None = None,
) -> str:
    LOGGER.debug(f"usersession_dirdel() {user_component=}")

    job_component_name: str | None = None
    if user_component is not None:
        job_component_name = _USER_SESSION_DEFS[user_component].job_component_name

    session_dir = UserSessionDirectory(authenticated_user.get_user_id())
    session_dir.delete_session_info(job_component_name)

    session_info_arr = session_dir.get_session_info_arr(None)
    LOGGER.debug("======================")
    for session_info in session_info_arr:
        LOGGER.debug(f"{session_info=}")
    LOGGER.debug("======================")

    return "Session info deleted"


@router.get("/bgtask")
async def bgtask() -> str:
    LOGGER.debug(f"bgtask() - start")

    async def funcThatThrows() -> None:
        raise ValueError("This is a test error")

    async def funcThatLogs(msg: str) -> None:
        LOGGER.debug(f"This is a test log {msg=}")

    run_in_background_task(funcThatThrows())
    run_in_background_task(funcThatLogs(msg="HELO HELLO"))

    LOGGER.debug(f"bgtask() - done")

    return "Background tasks were run"


@router.get("/ri_surf")
async def ri_surf(
    authenticated_user: Annotated[AuthenticatedUser, Depends(AuthHelper.get_authenticated_user)],
) -> str:
    LOGGER.debug(f"ri_surf() - start")

    case_uuid = "485041ce-ad72-48a3-ac8c-484c0ed95cf8"
    ensemble_name = "iter-0"
    realization = 1
    # grid_name = "Simgrid"
    # property_name = "PORO"
    grid_name = "Geogrid"
    property_name = "Region"

    ijk_index_filter = IJKIndexFilter(min_i=0, max_i=0, min_j=0, max_j=0, min_k=0, max_k=0)

    grid_service = await UserGrid3dService.create_async(authenticated_user, case_uuid)
    await grid_service.get_grid_geometry_async(ensemble_name, realization, grid_name, ijk_index_filter)
    await grid_service.get_mapped_grid_properties_async(
        ensemble_name, realization, grid_name, property_name, ijk_index_filter
    )

    return "OK"


@router.get("/ri_isect")
async def ri_isect(
    authenticated_user: Annotated[AuthenticatedUser, Depends(AuthHelper.get_authenticated_user)],
) -> str:
    LOGGER.debug(f"ri_isect() - start")

    case_uuid = "485041ce-ad72-48a3-ac8c-484c0ed95cf8"
    ensemble_name = "iter-0"
    realization = 1
    # grid_name = "Simgrid"
    # property_name = "PORO"
    grid_name = "Geogrid"
    property_name = "Region"

    # Polyline for testing
    # fmt:off
    xy_arr = [
        463156.911, 5929542.294,
        463564.402, 5931057.803,
        463637.925, 5931184.235,
        463690.658, 5931278.837,
        463910.452, 5931688.122,
        464465.876, 5932767.761,
        464765.876, 5934767.761,
    ]
    # fmt:on

    grid_service = await UserGrid3dService.create_async(authenticated_user, case_uuid)
    await grid_service.get_polyline_intersection_async(ensemble_name, realization, grid_name, property_name, xy_arr)

    return "OK"


@router.get("/blobtest", response_class=HTMLResponse)
async def blobtest(
    authenticated_user: Annotated[AuthenticatedUser, Depends(AuthHelper.get_authenticated_user)],
    agg_col: Annotated[str | None, Query()] = None,
    get_col: Annotated[list[str] | None, Query()] = None,
    case_uuid: Annotated[str, Query()] = "11167ec3-41f7-452c-8a08-38466df6bb97",
    iteration_name: Annotated[str, Query()] = "iter-0",
    realization: Annotated[int, Query()] = 1,
) -> str:
    LOGGER.debug(f"blobtest() - start")

    table_name = None

    """
    # case_uuid = "11167ec3-41f7-452c-8a08-38466df6bb97" # webviz_case_2
    # iteration_name = "iter-0"
    # realization = 1
    # table_name = None
    # #table_name = "DROGON"

    case_uuid = "e2e2c560-788a-4e92-93a3-3ba7afe2c796" # ffgas_rnb2024
    iteration_name = "iter-0"
    realization = 1
    table_name = None
    #table_name = "TROLL_GAS_HIST"

    agg_col = None
    agg_col = "FGPR"

    get_col = None
    #get_col = ["REAL"]
    #get_col = ["DATE"]
    #get_col = ["DATE", "FGPR"]
    #get_col = ["FGPR"]
    #get_col = ['DATE', 'YEARS', 'DAY', 'MONTH', 'YEAR', 'FGIP', 'FGPR', 'FGPRH', 'FGPT', 'FGPTH', 'FGIR', 'FGIRH', 'FGIT', 'FGITH', 'FGCR', 'FGCT', 'FGSR', 'FGST', 'FGPP', 'FWIP', 'FWGR', 'FWPR', 'FWPT', 'FWPP', 'FVPR', 'FVPT', 'FVIR', 'FVIT', 'FPR', 'FNQR', 'FNQT']
    """
    
    LOGGER.debug(f"{case_uuid=}")
    LOGGER.debug(f"{iteration_name=}")
    LOGGER.debug(f"{realization=}")
    LOGGER.debug(f"{agg_col=}")
    LOGGER.debug(f"{get_col=}")

    sumo_access_token = authenticated_user.get_sumo_access_token()
    sumo_client = SumoClient(env=config.SUMO_ENV, token=sumo_access_token, interactive=False)

    if agg_col is not None:
        data_format = "parquet"
        blob_id = await find_all_real_combined_summary_table_blob_id(sumo_client, case_uuid, iteration_name, table_name, agg_col)
    else:
        data_format = "feather"
        blob_id = await find_single_real_summary_table_blob_id(sumo_client, case_uuid, iteration_name, table_name, realization)


    sas_token, blob_store_base_uri = get_sas_token_and_blob_store_base_uri_for_case(sumo_access_token, case_uuid)
    LOGGER.debug(f"{sas_token=}")
    LOGGER.debug(f"{blob_store_base_uri=}")

    timer = PerfTimer()

    blob_endpoint = blob_store_base_uri.rpartition("/")[0]
    conn_string = f"BlobEndpoint={blob_endpoint};SharedAccessSignature={sas_token}"
    fs = adlfs.AzureBlobFileSystem(connection_string=conn_string)
    et_create_fs_ms = timer.lap_ms()
    LOGGER.debug(f"{fs=}")


    blob_path = f"{case_uuid}/{blob_id}"

    size_bytes = fs.size(path=blob_path)
    size_mb = size_bytes/1024/1024
    LOGGER.debug(f"{size_mb=:.2f}")
    et_get_blob_size_ms = timer.lap_ms()


    # TRY AND DO A RAW DOWNLOAD FOR COMPARISON!!!!
    # TRY AND DO A RAW DOWNLOAD FOR COMPARISON!!!!
    et_download_blob_with_sumo_ms = -1
    byte_stream_from_sumo: io.BytesIO = await download_blob_using_sumo(sumo_client, blob_id)
    if data_format == "parquet":
        # parquet_file = pa.parquet.ParquetFile(byte_stream_from_sumo)
        # LOGGER.debug(f"{parquet_file.metadata=}")
        # LOGGER.debug(f"{parquet_file.metadata.row_group(0).column(0).compression=}")
        table_from_sumo = pq.read_table(byte_stream_from_sumo, columns=get_col)
    else:
        table_from_sumo = pf.read_table(byte_stream_from_sumo, columns=get_col)

        # pa.feather.write_feather(table_from_sumo, "~/dump_compressed.feather")
        # pa.feather.write_feather(table_from_sumo, "~/dump_uncompressed.feather", compression="uncompressed")
        # pq.write_table(table_from_sumo, "~/dump_compressed.parquet")

    et_download_blob_with_sumo_ms = timer.lap_ms()



    if data_format == "parquet":
        table = pq.read_table(blob_path, filesystem=fs, columns=get_col)
    else:
        the_file = fs.open(blob_path, "rb")
        table = pf.read_table(the_file, columns=get_col)

    # dataset = ds.dataset(blob_path, filesystem=fs, format=data_format)
    # #table = dataset.to_table(columns=columns_to_get, filter=ds.field("REAL") == 3)
    # table = dataset.to_table(columns=columns_to_get)

    et_get_data_table_ms = timer.lap_ms()

    #LOGGER.debug(table.schema)
    #LOGGER.debug(table.column_names)
    #df = table.to_pandas()


    elapsed_s = timer.elapsed_s()

    retstr = f"DONE - {blob_path}"
    retstr += "<br>"
    retstr += f"<br>elapsed_s={elapsed_s:.3f}"
    retstr += f"<br>{table.shape=}"
    retstr += f"<br>{table_from_sumo.shape=}"
    retstr += f"<br>{et_create_fs_ms=}"
    retstr += f"<br>{et_get_blob_size_ms=}"
    retstr += f"<br>{et_download_blob_with_sumo_ms=}"
    retstr += f"<br>{et_get_data_table_ms=}"
    retstr += "<br><br>"

    LOGGER.debug(f"blobtest() - done")

    return retstr


async def find_single_real_summary_table_blob_id(sumo_client: SumoClient, case_uuid: str, iteration_name: str, table_name: str | None, realization: int) -> str:
    LOGGER.debug("find_single_real_summary_table_blob_id() - start")

    myquery = {
        "query": {
            "bool": {
                "must": [
                    {"match": {"_sumo.parent_object.keyword": case_uuid}},
                    {"match": {"class": "table"}},
                    {"match": {"fmu.iteration.name.keyword": iteration_name}},
                    {"match": {"fmu.context.stage.keyword": "realization"}},
                    {"match": {"fmu.realization.id": realization}},
                    #{"match": {"data.name.keyword": table_name}},
                    #{"match": {"data.content.keyword": "timeseries"}},
                    {"match": {"data.tagname.keyword": "summary"}},
                ]
            }
        },
    }

    if table_name is not None:
        myquery["query"]["bool"]["must"].append({"match": {"data.name.keyword": table_name}})

    response = await sumo_client.post_async("/search", json=myquery)
    response_as_json = response.json()

    # LOGGER.debug("-----------------")
    # delete_key_from_dict_recursive(response_as_json, "parameters")
    # delete_key_from_dict_recursive(response_as_json, "columns")
    # LOGGER.debug(json.dumps(response_as_json, indent=2))
    # LOGGER.debug("-----------------")

    hits = response_as_json["hits"]["hits"]
    if len(hits) != 1:
        raise ValueError(f"Expected 1 hit, got {len(hits)}")

    blob_id = hits[0]["_source"]["_sumo"]["blob_name"]
    LOGGER.debug(f"find_single_real_summary_table_blob_id() - {blob_id=}")

    blob_size_bytes = hits[0]["_source"]["_sumo"].get("blob_size")
    blob_size_mb = blob_size_bytes / 1024 / 1024 if blob_size_bytes is not None else -1
    LOGGER.debug(f"find_single_real_summary_table_blob_id() - {blob_size_mb=:.2f}")

    LOGGER.debug("find_single_real_summary_table_blob_id() - done")

    return blob_id


async def find_all_real_combined_summary_table_blob_id(sumo_client: SumoClient, case_uuid: str, iteration_name: str, table_name: str | None, agg_column_name: str) -> str:
    LOGGER.debug("find_all_real_combined_summary_table_blob_id() - start")

    myquery = {
        "query": {
            "bool": {
                "must": [
                    {"match": {"_sumo.parent_object.keyword": case_uuid}},
                    {"match": {"class.keyword": "table"}},
                    {"match": {"fmu.iteration.name.keyword": iteration_name}},
                    {"match": {"fmu.context.stage.keyword": "iteration"}},
                    {"match": {"fmu.aggregation.operation.keyword": "collection"}},
                    #{"match": {"data.name.keyword": table_name}},
                    #{"match": {"data.content.keyword": "timeseries"}},
                    {"match": {"data.tagname.keyword": "summary"}},
                    {"match": {"data.spec.columns.keyword": agg_column_name}},
                ]
            }
        },
    }

    if table_name is not None:
        myquery["query"]["bool"]["must"].append({"match": {"data.name.keyword": table_name}})

    response = await sumo_client.post_async("/search", json=myquery)
    response_as_json = response.json()

    # LOGGER.debug("-----------------")
    # delete_key_from_dict_recursive(response_as_json, "parameters")
    # delete_key_from_dict_recursive(response_as_json, "realization_ids")
    # LOGGER.debug(json.dumps(response_as_json, indent=2))
    # LOGGER.debug("-----------------")

    hits = response_as_json["hits"]["hits"]
    if len(hits) != 1:
        raise ValueError(f"Expected 1 hit, got {len(hits)}")

    blob_id = hits[0]["_source"]["_sumo"]["blob_name"]
    LOGGER.debug(f"find_all_real_combined_summary_table_blob_id() - {blob_id=}")

    blob_size_bytes = hits[0]["_source"]["_sumo"].get("blob_size")
    blob_size_mb = blob_size_bytes / 1024 / 1024 if blob_size_bytes is not None else -1
    LOGGER.debug(f"find_all_real_combined_summary_table_blob_id() - {blob_size_mb=:.2f}")

    LOGGER.debug("find_all_real_combined_summary_table_blob_id() - done")

    return blob_id


async def download_blob_using_sumo(sumo_client: SumoClient, blob_id: str) -> io.BytesIO:
    LOGGER.debug("download_blob_using_sumo() - start")

    res = await sumo_client.get_async(f"/objects('{blob_id}')/blob")
    byte_stream = io.BytesIO(res.content)
    num_bytes = byte_stream.getbuffer().nbytes
    num_mb = num_bytes / 1024 / 1024
    LOGGER.debug(f"download_blob_using_sumo() - {num_mb=:.2f}")

    LOGGER.debug("download_blob_using_sumo() - done")

    return byte_stream


def delete_key_from_dict_recursive(d, key_to_delete):
    if isinstance(d, dict):
        for key, value in list(d.items()):
            if key == key_to_delete:
                del d[key]
            else:
                delete_key_from_dict_recursive(value, key_to_delete)
    elif isinstance(d, list):
        for item in d:
            delete_key_from_dict_recursive(item, key_to_delete)