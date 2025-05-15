import asyncio
import logging
from typing import Annotated

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query, Path

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
async def get_usersession_call(
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
async def get_usersession_radixlist(user_component: UserComponent) -> list:
    LOGGER.debug(f"usersession_radixlist() {user_component=}")

    session_def = _USER_SESSION_DEFS[user_component]
    radix_job_api = RadixJobApi(session_def.job_component_name, session_def.port)

    job_list = await radix_job_api.get_all_jobs()

    LOGGER.debug("======================")
    LOGGER.debug(job_list)
    LOGGER.debug("======================")

    return job_list


@router.get("/usersession/{user_component}/radixcreate")
async def get_usersession_radixcreate(user_component: UserComponent) -> str:
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
async def get_usersession_radixdelete(user_component: UserComponent) -> str:
    LOGGER.debug(f"usersession_radixdelete() {user_component=}")

    session_def = _USER_SESSION_DEFS[user_component]
    radix_job_api = RadixJobApi(session_def.job_component_name, session_def.port)

    await radix_job_api.delete_all_jobs()

    return "Delete done"


@router.get("/usersession/dirlist")
async def get_usersession_dirlist(
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
async def get_usersession_dirdel(
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
async def get_bgtask() -> str:
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
async def get_ri_surf(
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
        ensemble_name, realization, grid_name, property_name, None, ijk_index_filter
    )

    return "OK"


@router.get("/ri_isect")
async def get_ri_isect(
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
    await grid_service.get_polyline_intersection_async(
        ensemble_name, realization, grid_name, property_name, None, xy_arr
    )

    return "OK"



import datetime
import os
from fastapi import Request
from primary.auth.auth_helper import AuthHelper
from primary.celery_worker.tasks import test_tasks
from primary.middleware.add_browser_cache import no_cache
from celery.result import AsyncResult

from azure.storage.blob import BlobServiceClient, ContainerClient, ContentSettings

from ..surface import schemas



@router.get("/celery_test")
@no_cache
async def get_celery_test() -> str:
    LOGGER.info(f"celery_test start !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    result: AsyncResult = test_tasks.capitalize_string.delay("my lowercase string")

    LOGGER.info(f"celery_test result: {result.id=}, {result.status=}, {result.result=}")

    LOGGER.info(f"celery_test end !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")

    return f"celery_test: time: {datetime.datetime.now()}  {result.id=}, {result.status=}"


@router.get("/celery_surf")
@no_cache
async def get_celery_surf(request: Request, surf_addr_str: str) -> str:
    LOGGER.info(f"get_celery_surf start !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")

    authenticated_user = AuthHelper.get_authenticated_user(request)

    # case_uuid = "b182dc88-9bb5-4076-ad33-1905c3d8c10b"
    # ensemble_name = "iter-0"
    # result: AsyncResult = test_tasks.surface_meta.delay(authenticated_user._sumo_access_token, case_uuid, ensemble_name)

    #surf_addr_str = "REAL~~aea92953-b5a3-49c6-9119-5ab34dd10bc4~~iter-0~~VOLANTIS GP. Top~~DS_extract_geogrid~~0"
    #surf_addr_str = "STAT~~aea92953-b5a3-49c6-9119-5ab34dd10bc4~~iter-0~~VOLANTIS%20GP.%20Top~~DS_extract_geogrid~~MEAN~~*"

    result: AsyncResult = test_tasks.surface_from_addr.delay(authenticated_user._sumo_access_token, surf_addr_str)

    timeout = 10.0  # seconds
    poll_interval = 0.5  # seconds
    waited = 0

    while not result.ready() and waited < timeout:
        await asyncio.sleep(poll_interval)
        waited += poll_interval

    LOGGER.info(result)
    LOGGER.info(f"get_celery_surf task: {waited=} - {result.id=}, {result.status=}, {result.result=}")

    LOGGER.info(f"get_celery_surf end !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")

    if not result.successful():
        if result.failed():
            raise HTTPException(status_code=500, detail="f[{datetime.datetime.now()]) {str(result.result)}")
        else:
            raise HTTPException(status_code=202, detail=f"Task still running  ({datetime.datetime.now()})")


    blob_name_without_extension = str(result.result)
    msgpack_blob_name = blob_name_without_extension + ".msgpack"

    blob_download_ok = False
    try:
        azure_storage_connection_string = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
        container_name = "celery-results"

        blob_service_client = BlobServiceClient.from_connection_string(azure_storage_connection_string)
        container_client: ContainerClient = blob_service_client.get_container_client(container_name)
        blob_client = container_client.get_blob_client(blob=msgpack_blob_name)
        download_stream = blob_client.download_blob()
        blob_data = download_stream.readall()

        ret_data: schemas.SurfaceDataFloat = test_tasks.msgpack_to_pydantic(schemas.SurfaceDataFloat, blob_data)
        #return ret_data.model_dump_json()

        blob_download_ok = True

    except Exception as e:
        LOGGER.error(f"Failed to connect to Azure Blob Storage")

    return f"get_celery_surf: time: {datetime.datetime.now()}  {result.id=}, {result.status=}, {result.result=}, {blob_download_ok=}"



