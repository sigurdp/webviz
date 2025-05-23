import asyncio
import logging
from typing import Annotated, List, Optional, Literal, Union, Type, get_origin, get_args
from types import UnionType

from fastapi import APIRouter, Depends, HTTPException, Query, Response, Body, status
from webviz_pkg.core_utils.perf_metrics import PerfMetrics

from primary.services.sumo_access.surface_access import SurfaceAccess
from primary.services.utils.statistic_function import StatisticFunction
from primary.services.utils.authenticated_user import AuthenticatedUser
from primary.auth.auth_helper import AuthHelper
from primary.services.surface_query_service.surface_query_service import RealizationSampleResult
from primary.utils.response_perf_metrics import ResponsePerfMetrics

from . import converters
from . import schemas
from . import dependencies

from .surface_address import RealizationSurfaceAddress, ObservedSurfaceAddress, StatisticalSurfaceAddress
from .surface_address import decode_surf_addr_str

from hashlib import sha256
import redis
import datetime
import os
from typing import Generic, TypeVar
from pydantic import BaseModel
from azure.storage.blob.aio import BlobServiceClient, ContainerClient, StorageStreamDownloader
from celery.result import AsyncResult
from primary.celery_worker.tasks import test_tasks
from primary.services.utils.otel_span_tracing import otel_span_decorator, start_otel_span, start_otel_span_async
from primary import config


LOGGER = logging.getLogger(__name__)

router = APIRouter()

redis_store = redis.Redis.from_url(config.REDIS_CACHE_URL, db=5, decode_responses=True)


"""
@router.get("/celery_surface_data", description="Get surface data via Celery.")
async def get_celery_surface_data(
    # fmt:off
    response: Response,
    authenticated_user: Annotated[AuthenticatedUser, Depends(AuthHelper.get_authenticated_user)],
    surf_addr_str: Annotated[str, Query(description="Surface address string, supported address types are *REAL*, *OBS* and *STAT*")],
    # fmt:on
) -> schemas.SurfaceDataFloat:
    perf_metrics = ResponsePerfMetrics(response)

    access_token = authenticated_user.get_sumo_access_token()

    LOGGER.info(f"Launching celery task to get surface data for address: {surf_addr_str}")

    addr = decode_surf_addr_str(surf_addr_str)
    if not isinstance(addr, RealizationSurfaceAddress):
        raise HTTPException(status_code=404, detail="Endpoint only supports address types REAL")

    with start_otel_span("schedule-task"):
        result: AsyncResult = test_tasks.surface_from_addr.delay(access_token, surf_addr_str)

    timeout = 10.0  # seconds
    poll_interval = 0.1  # seconds
    waited = 0
    async with start_otel_span_async("poll-task"):
        while not result.ready() and waited < timeout:
            await asyncio.sleep(poll_interval)
            waited += poll_interval
            LOGGER.debug(f"waiting for task {result.id} to complete... {waited=:.1f} {result.status=} {result.state=} {result.info=}")

    if not result.successful():
        if result.failed():
            raise HTTPException(status_code=500, detail=f"[{datetime.datetime.now()}]) {str(result.result)}")
        else:
            raise HTTPException(status_code=202, detail=f"Task still running  ({datetime.datetime.now()})")


    blob_name_without_extension = str(result.result)
    msgpack_blob_name = blob_name_without_extension + ".msgpack"
    blob_download_ok = False

    async with start_otel_span_async("download-task-result"):
        try:
            blob_bytes = await _download_celery_result_blob(msgpack_blob_name)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to download result from blob storage")

    with start_otel_span("unpack-msg"):
        ret_data: schemas.SurfaceDataFloat = test_tasks.msgpack_to_pydantic(schemas.SurfaceDataFloat, blob_bytes)

    LOGGER.info(f"Got {addr.address_type} surface in: {perf_metrics.to_string()}")

    return ret_data
"""



class TaskProgress(BaseModel):
    progress: str
    task_id: str


@router.get("/celery_polling_surface_data", description="Get surface data via Celery.")
async def get_celery_polling_surface_data(
    # fmt:off
    response: Response,
    authenticated_user: Annotated[AuthenticatedUser, Depends(AuthHelper.get_authenticated_user)],
    surf_addr_str: Annotated[str, Query(description="Surface address string, supported address types are *REAL*, *OBS* and *STAT*")],
    # fmt:on
) -> schemas.SurfaceDataFloat | TaskProgress:
    perf_metrics = ResponsePerfMetrics(response)

    user_id = authenticated_user.get_user_id()
    access_token = authenticated_user.get_sumo_access_token()

    addr = decode_surf_addr_str(surf_addr_str)
    if not isinstance(addr, RealizationSurfaceAddress):
        raise HTTPException(status_code=404, detail="Endpoint only supports address types REAL")

    task_key = _make_task_key(user_id, surf_addr_str)

    celery_task_id_key = f"{task_key}:celery_task_id"
    celery_task_id = redis_store.get(celery_task_id_key)

    if not celery_task_id:
        # Start a new task
        with start_otel_span("schedule-task"):
            celery_result: AsyncResult = test_tasks.surface_from_addr.delay(access_token, surf_addr_str)

        celery_task_id = celery_result.id
        redis_store.set(celery_task_id_key, celery_task_id)
        redis_store.expire(celery_task_id_key, 600)

    celery_result = AsyncResult(celery_task_id)

    # Note that here ready includes both success and failure (and revoked)
    if not celery_result.ready():
        response.headers["Cache-Control"] = "no-store"
        response.status_code = status.HTTP_202_ACCEPTED
        return TaskProgress(task_id=celery_task_id, progress=f"Working: {celery_result.state} ({celery_result.info=}) - [{datetime.datetime.now()}]")

    if not celery_result.successful():
        if celery_result.failed():
            raise HTTPException(status_code=500, detail=f"Task failed [{datetime.datetime.now()}] {str(celery_result.result)}")
        else:
            raise HTTPException(status_code=202, detail=f"Task trouble  [{datetime.datetime.now()}]")


    blob_name_without_extension = str(celery_result.result)
    msgpack_blob_name = blob_name_without_extension + ".msgpack"

    async with start_otel_span_async("download-task-result"):
        try:
            blob_bytes = await _download_celery_result_blob(msgpack_blob_name)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to download result from blob storage")

    with start_otel_span("unpack-msg"):
        ret_data: schemas.SurfaceDataFloat = test_tasks.msgpack_to_pydantic(schemas.SurfaceDataFloat, blob_bytes)

    LOGGER.info(f"Got {addr.address_type} surface in: {perf_metrics.to_string()}")

    return ret_data



def _make_task_key(user_id: str, input_data: str) -> str:
    key_hash = sha256(input_data.encode()).hexdigest()
    return f"user:{user_id}:task:{key_hash}"



_CONTAINER_CLIENT: ContainerClient | None = None

@otel_span_decorator()
async def _download_celery_result_blob(blob_name: str) -> bytes:
    global _CONTAINER_CLIENT

    if not _CONTAINER_CLIENT:
        azure_storage_connection_string = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
        container_name = "celery-results"

        with start_otel_span("get-container-client"):
            blob_service_client = BlobServiceClient.from_connection_string(azure_storage_connection_string)
            _CONTAINER_CLIENT = blob_service_client.get_container_client(container_name)

    with start_otel_span("get-client"):
        blob_client = _CONTAINER_CLIENT.get_blob_client(blob=blob_name)

    async with start_otel_span_async("download"):
        download_stream: StorageStreamDownloader[bytes] = await blob_client.download_blob()
        blob_byte_data: bytes = await download_stream.readall()

    return blob_byte_data



# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

T = TypeVar("T")


class ErrorInfo(BaseModel):
    message: str
    code: int

class ProgressInfo(BaseModel):
    progress_message: str


class SuccessTaskResp(BaseModel, Generic[T]):
    status: Literal["success"]
    data: T

class InProgressTaskResp(BaseModel):
    status: Literal["inProgress"]
    progress: ProgressInfo

class ErrorTaskResp(BaseModel):
    status: Literal["error"]
    error: ErrorInfo


def make_response_envelope(result_type: Type[T]) -> Type:
    return Union[
        SuccessTaskResp[result_type],
        InProgressTaskResp,
        ErrorTaskResp,
    ]


def make_response_envelope(result_type: Type) -> Type:
    origin = get_origin(result_type)
    if origin in {Union, UnionType}:
        result_types = get_args(result_type)
    else:
        result_types = (result_type,)

    success_variants = tuple(SuccessTaskResp[rt] for rt in result_types)

    return Union[*success_variants, InProgressTaskResp, ErrorTaskResp]



class MyAscResult(BaseModel):
    format: Literal["asc"]
    the_string: str
    the_value: int


class MyBinResult(BaseModel):
    format: Literal["bin"]
    base64_str: str



@router.get("/sigurd_experiment", response_model=make_response_envelope(MyAscResult | MyBinResult))
async def get_sigurd_experiment():
    
    return SuccessTaskResp[MyAscResult](
        status="success",
        data=MyAscResult(
            the_string="Hello, world!",
            the_value=42
        )
    )






"""
class ParamModel1(BaseModel):
    myString: str
    myInt: int

class ParamModel2(BaseModel):
    myFloat: float


@router.get("/query_param_model")
async def get_query_param_model(
    param: Annotated[ParamModel2, Query()],
    param2: Annotated[int, Query()],
) -> str:
    return f"Yes: {datetime.datetime.now()}"
"""




"""
class GroupA(BaseModel):
    a1: Optional[str] = None
    a2: Optional[int] = None

class GroupB(BaseModel):
    b1: Optional[float] = None
    b2: Optional[bool] = None


def get_group_a(
    a1: Optional[str] = Query(default=None),
    a2: Optional[int] = Query(default=None),
) -> GroupA:
    return GroupA(a1=a1, a2=a2)

def get_group_b(
    b1: Optional[float] = Query(default=None),
    b2: Optional[bool] = Query(default=None),
) -> GroupB:
    return GroupB(b1=b1, b2=b2)


from fastapi import Depends, HTTPException
from typing import Union

QueryGroup = Union[GroupA, GroupB]

def resolve_exclusive_group(
    group_a: GroupA = Depends(get_group_a),
    group_b: GroupB = Depends(get_group_b),
) -> QueryGroup:
    a_set = any(v is not None for v in group_a.dict().values())
    b_set = any(v is not None for v in group_b.dict().values())

    if a_set and b_set:
        raise HTTPException(status_code=400, detail="Only one parameter group can be set")
    if not a_set and not b_set:
        raise HTTPException(status_code=400, detail="One parameter group must be set")

    return group_a if a_set else group_b

@router.get("/items/")
def read_items(group: QueryGroup = Depends(resolve_exclusive_group)):
    return {
        "group_used": type(group).__name__,
        "values": group.dict()
    }

"""