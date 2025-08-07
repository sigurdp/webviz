import datetime
import logging
from hashlib import sha256
from types import UnionType
from typing import Annotated, Generic, List, Literal, Optional, Type, TypeVar, Union, get_args, get_origin

from celery.result import AsyncResult
from fastapi import APIRouter, Depends, HTTPException, Query, Response, status
from pydantic import BaseModel, TypeAdapter
from webviz_pkg.core_utils.perf_metrics import PerfMetrics

from primary.auth.auth_helper import AuthHelper
from primary.celery_worker.tasks import surface_tasks
from primary.celery_worker.tasks.surface_tasks import ExpectedTaskRes, TaskResOk, TaskResErr
from primary.services.utils.authenticated_user import AuthenticatedUser
from primary.services.utils.otel_span_tracing import otel_span_decorator, start_otel_span, start_otel_span_async
from primary.services.utils.task_meta_tracker import get_task_meta_tracker_for_user
from primary.services.utils.temp_user_store import get_temp_user_store_for_user
from primary.utils.response_perf_metrics import ResponsePerfMetrics

from .._shared.long_running_operations import (LroErrorInfo, LroErrorResp, LroInProgressResp, LroProgressInfo,
                                               LroSuccessResp)
from . import schemas
from .surface_address import RealizationSurfaceAddress, decode_surf_addr_str

LOGGER = logging.getLogger(__name__)

router = APIRouter()


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

def _extract_state_and_progress_msg_from_celery_result(celery_result: AsyncResult) -> tuple[str, str | None]:
    state_str = celery_result.state
    progress_msg = None

    # We use our own custom "IN_PROGRESS" state to communicate the progress message
    # The progress message is set as a metadata dict on the "IN_PROGRESS" state inside the task,
    # and the dict is accessible here through the result property of the AsyncResult
    if state_str == "IN_PROGRESS":
        result = celery_result.result
        if isinstance(result, dict) and "progress_msg" in result:
            progress_msg = result["progress_msg"]

    return state_str, progress_msg


@router.get("/celery_surface_data", description="Hybrid get of surface data via Celery.")
async def get_celery_surface_data(
    response: Response,
    authenticated_user: Annotated[AuthenticatedUser, Depends(AuthHelper.get_authenticated_user)],
    surf_addr_str: Annotated[str, Query()],
) -> LroSuccessResp[schemas.SurfaceDataFloat] | LroInProgressResp | LroErrorResp:
    perf_metrics = ResponsePerfMetrics(response)

    addr = decode_surf_addr_str(surf_addr_str)
    if not isinstance(addr, RealizationSurfaceAddress):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Endpoint only supports address types REAL")

    user_id = authenticated_user.get_user_id()
    access_token = authenticated_user.get_sumo_access_token()

    param_hash = sha256(surf_addr_str.encode()).hexdigest()
    store_key = f"celery_polling_surface_data__{param_hash}"

    user_store = get_temp_user_store_for_user(authenticated_user)

    # Easy store/cache lookup first.
    # If we find the payload, we just return it
    existing_surf_data = await user_store.get_pydantic_model(key=store_key, model_class=schemas.SurfaceDataFloat, format="msgpack")
    if existing_surf_data:
        return LroSuccessResp(status="success", data=existing_surf_data)

    tracker = get_task_meta_tracker_for_user(authenticated_user)
    existing_celery_task_id = await tracker.get_task_id_by_fingerprint_async(param_hash)

    if existing_celery_task_id:
        # We have an existing task id for this request payload, query the state of the celery task
        celery_result = AsyncResult(existing_celery_task_id)

        # Note that here, ready() means that the task is done (includes both success, failure and revoked)
        if celery_result.ready():
            # Task is done, but we found no result in the store in the code above. We will proceed to report this back as an error.
            # First, remove the mapping so that we'll trigger submit of a new celery task the next time
            await tracker.delete_fingerprint_to_task_mapping_async(fingerprint=param_hash)

            if celery_result.successful():
                # Task was actually successful, but we did not find a result in the store.
                # This could be either an error or the task may have succeeded just after we checked the store.
                # For now, we'll report it back as an error either way.

                """
                # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                # Experiment with separating expected results and error from exceptions
                # See ExpectedTaskRes over in surface_tasks.py
                # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                #task_res = surface_tasks.TaskRes.model_validate(celery_result.result)
                LOGGER.info("-------------------------------------------")
                ta: TypeAdapter[ExpectedTaskRes] = TypeAdapter(ExpectedTaskRes)
                task_res = ta.validate_python(celery_result.result)
                LOGGER.info(task_res)
                LOGGER.info("-------------------------------------------")

                if isinstance(task_res, TaskResErr):
                    return LroErrorResp(
                        status="failure",
                        error=LroErrorInfo(message=f"Task execution failed: {task_res.message}, task_id={existing_celery_task_id}")
                    )

                # If we get here, the task was successful but we did not find a result in the store.
                # Should we do another query against the result store?
                # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                """

                LOGGER.error(f"Celery task is successful but no result was found in store [task_id={existing_celery_task_id}]")
                raise HTTPException(status_code=500, detail=f"Task successful but no result found, task_id={existing_celery_task_id}")
            
            if celery_result.failed():
                LOGGER.error(f"Celery task failed: {celery_result.result} [task_id={existing_celery_task_id}]")
                
                # !!!!!!
                # To discuss:
                # What should the http status code for such an hybrid endpoint be in this case?
                # If we think in terms of polling the status of a straight long running submit/poll endpoint, then this would be a 200.
                # But if we think in terms of a hybrid approach (more similar to an ordinary get), this would probably be 500
                response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR

                # Also to discuss, what goes into the error message here?
                # We should not propagate the error from the failed task since that may leak exception messages from the celery task worker.
                return LroErrorResp(
                    status="failure",
                    error=LroErrorInfo(message=f"Task execution failed, task_id={existing_celery_task_id}",)
                )
            
            # Not sure what other states we may end up in here, so just add a general guard
            raise HTTPException(status_code=500, detail=f"Unexpected state in completed task, task_id={existing_celery_task_id}")
        
        else:
            response.headers["Cache-Control"] = "no-store"
            state_str, details_str = _extract_state_and_progress_msg_from_celery_result(celery_result)
            return LroInProgressResp(
                status="in_progress",
                operation_id=existing_celery_task_id,
                progress=LroProgressInfo(progress_message=f"Task in progress: state={state_str}, details={details_str}  [{datetime.datetime.now()}]"),
            )

    # Start a new celery task
    with start_otel_span("schedule-task"):
        new_celery_task: AsyncResult = surface_tasks.surface_from_addr.delay(
            user_id=user_id,
            sumo_access_token=access_token,
            surf_addr_str=surf_addr_str,
            target_store_key=store_key,
            )

    celery_task_id = new_celery_task.id
    await tracker.register_task_with_fingerprint_async(task_system="celery", task_id=celery_task_id, fingerprint=param_hash, expected_store_key=store_key)

    response.headers["Cache-Control"] = "no-store"
    response.status_code = status.HTTP_202_ACCEPTED
    state_str, details_str = _extract_state_and_progress_msg_from_celery_result(new_celery_task)
    return LroInProgressResp(
        status="in_progress",
        operation_id=celery_task_id,
        progress=LroProgressInfo(progress_message=f"Task submitted: state={state_str}, details={details_str}  [{datetime.datetime.now()}]"),
    )




# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

T = TypeVar("T")


# def make_response_envelope(result_type: Type[T]) -> Type:
#     return Union[
#         LroSuccessResp[result_type],
#         LroInProgressResp,
#         LroErrorResp,
#     ]


def make_response_envelope(result_type: Type) -> Type:
    origin = get_origin(result_type)
    if origin in {Union, UnionType}:
        result_types = get_args(result_type)
    else:
        result_types = (result_type,)

    success_variants = tuple(LroSuccessResp[rt] for rt in result_types)

    return Union[*success_variants, LroInProgressResp, LroErrorResp]


class MyAscResult(BaseModel):
    format: Literal["asc"]
    the_string: str
    the_value: int


class MyBinResult(BaseModel):
    format: Literal["bin"]
    base64_str: str



@router.get("/sigurd_experiment", response_model=make_response_envelope(MyAscResult | MyBinResult))
async def get_sigurd_experiment():
    
    return LroSuccessResp[MyAscResult](
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