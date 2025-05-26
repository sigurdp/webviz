import asyncio
import logging
from enum import Enum
from hashlib import sha256
from typing import Generic, Literal, TypeVar

from fastapi import APIRouter, BackgroundTasks, HTTPException, status
from pydantic import BaseModel

from primary.middleware.add_browser_cache import no_cache


LOGGER = logging.getLogger(__name__)

router = APIRouter()



T = TypeVar("T")

class ErrorInfo(BaseModel):
    message: str

class ProgressInfo(BaseModel):
    progress_message: str


class LroInProgressResp(BaseModel):
    status: Literal["in_progress"]
    operation_id: str
    poll_url: str | None = None
    progress: ProgressInfo | None = None

class LroErrorResp(BaseModel):
    status: Literal["failure"]
    error: ErrorInfo

class LroSuccessResp(BaseModel, Generic[T]):
    status: Literal["success"]
    data: T



class MyResult(BaseModel):
    my_string: str



class TaskState(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"

_LAST_USED_TASK_ID = 0
_FAKE_TASK_QUEUE: dict[str, TaskState] = {}
_FAKE_TASK_RESULT_STORE: dict[str, MyResult] = {}

_PAYLOAD_HASH_TO_TASK_MAP: dict[str, str] = {}

def _generate_new_task_id() -> str:
    global _LAST_USED_TASK_ID
    _LAST_USED_TASK_ID += 1
    return str(_LAST_USED_TASK_ID)



def _concatenate_strings(a: str, b:str) -> str:
    return f"{a}+++{b}"

async def _concatenate_strings_task(task_id: str, delay: float, a: str, b:str) -> None:
    _FAKE_TASK_QUEUE[task_id] = TaskState.RUNNING

    if delay > 0:
        # Simulate a long-running task
        await asyncio.sleep(delay)

    res = _concatenate_strings(a, b)
    _FAKE_TASK_RESULT_STORE[task_id] = MyResult(my_string=res)
    _FAKE_TASK_QUEUE[task_id] = TaskState.COMPLETED





@router.post("/always_long_running")
@no_cache
async def post_always_long_running(background_tasks: BackgroundTasks, a: str, b:str, delay: float = 0) -> LroInProgressResp:
    task_id = _generate_new_task_id()
    _FAKE_TASK_QUEUE[task_id] = TaskState.PENDING
    background_tasks.add_task(_concatenate_strings_task, task_id, delay, a, b)

    return LroInProgressResp(
        status="in_progress",
        operation_id=task_id,
        poll_url=f"surface/always_long_running_status?task_id={task_id}",
        progress=ProgressInfo(progress_message="Task was added to queue")
    )

@router.get("/always_long_running_status")
@no_cache
async def get_always_long_running_status(task_id: str) -> LroInProgressResp | LroErrorResp | LroSuccessResp[MyResult]:
    task_state = _FAKE_TASK_QUEUE.get(task_id)
    if not task_state:
        raise HTTPException(status_code=500, detail="Unknown task_id")
    
    if task_state in [TaskState.PENDING, TaskState.RUNNING]:
        return LroInProgressResp(
            status="in_progress",
            operation_id=task_id,
            poll_url=f"surface/always_long_running_status?task_id={task_id}",
            progress=ProgressInfo(progress_message="Task is pending or running")
        )

    task_result = _FAKE_TASK_RESULT_STORE.get(task_id)
    if not task_result:
        raise HTTPException(status_code=500, detail="Task completed but no result found")

    return LroSuccessResp[MyResult](
        status="success",
        data=task_result,
    )



@router.get("/maybe_long_running")
@no_cache
async def get_maybe_long_running(background_tasks: BackgroundTasks, a: str, b:str, delay: float = 0) -> LroInProgressResp | LroErrorResp | LroSuccessResp[MyResult]:
    # Possibly simulate immediate response
    if delay <= 0:
        ret_str = _concatenate_strings(a, b)
        return LroSuccessResp[MyResult](
            status="success",
            data=MyResult(my_string=ret_str),
        )
    
    # Simulate a long-running task  
    payload_hash = sha256(f"{a}{b}{delay}".encode()).hexdigest()

    existing_task_id = _PAYLOAD_HASH_TO_TASK_MAP.get(payload_hash)
    if existing_task_id is not None:
        task_state = _FAKE_TASK_QUEUE.get(existing_task_id)
        if task_state in [TaskState.PENDING, TaskState.RUNNING]:
            return LroInProgressResp(
                status="in_progress",
                operation_id=existing_task_id,
                progress=ProgressInfo(progress_message="Task is pending or running")
            )
        if task_state == TaskState.COMPLETED:
            task_result = _FAKE_TASK_RESULT_STORE.get(existing_task_id)
            if task_result:
                return LroSuccessResp[MyResult](
                    status="success",
                    data=task_result,
                )

    new_task_id = _generate_new_task_id()
    _FAKE_TASK_QUEUE[new_task_id] = TaskState.PENDING
    _PAYLOAD_HASH_TO_TASK_MAP[payload_hash] = new_task_id
    background_tasks.add_task(_concatenate_strings_task, new_task_id, delay, a, b)

    return LroInProgressResp(
        status="in_progress",
        operation_id=new_task_id,
        progress=ProgressInfo(progress_message="A new task was added to queue")
    )



class MyAscResult(BaseModel):
    format: Literal["asc"]
    the_string: str
    the_value: int

class MyBinResult(BaseModel):
    format: Literal["bin"]
    base64_str: str

@router.get("/dummy_with_multiple_models")
@no_cache
async def get_dummy_with_multiple_models() -> LroInProgressResp | LroErrorResp | LroSuccessResp[MyAscResult] | LroSuccessResp[MyBinResult]:
    raise HTTPException(status.HTTP_501_NOT_IMPLEMENTED)

