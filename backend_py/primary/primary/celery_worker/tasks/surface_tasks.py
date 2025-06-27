import asyncio
import logging

import xtgeo
from celery import Task, shared_task
from webviz_pkg.core_utils.perf_metrics import PerfMetrics

from primary.celery_worker.celery_app import celery_app
from primary.routers.surface.converters import to_api_surface_data_float
from primary.routers.surface.surface_address import decode_surf_addr_str
from primary.services.sumo_access.surface_access import SurfaceAccess
from primary.services.utils.otel_span_tracing import otel_span_decorator, start_otel_span_async
from primary.services.utils.temp_user_store import get_temp_user_store_for_user_id

def _set_task_progress_msg(task: Task, progress_msg: str) -> None:
    # Here we use our own, custom "IN_PROGRESS" state with a status message in the meta data
    # Note that this call actually overwrites Celery task state with the specified state and meta.
    task.update_state(state="IN_PROGRESS", meta={"progress_msg": progress_msg})


@celery_app.task
def surface_meta(sumo_access_token: str, case_uuid: str, ensemble_name: str) -> str:
    logger = logging.getLogger(__name__)
    logger.info(f"surface_meta --- : {sumo_access_token=}, {case_uuid=}, {ensemble_name=}")

    access = SurfaceAccess.from_iteration_name(sumo_access_token, case_uuid, ensemble_name)
    sumo_surf_meta_set = asyncio.run(access.get_realization_surfaces_metadata_async())
    logger.info(sumo_surf_meta_set)

    return f"surface_meta OK"


@otel_span_decorator()
async def do_surface_from_addr_async(
    task: Task, user_id: str, sumo_access_token: str, surf_addr_str: str, target_store_key: str
) -> str | None:
    logger = logging.getLogger(__name__)
    #logger = get_task_logger(__name__)
    logger.debug(f"do_surface_from_addr_async() --- : {surf_addr_str=}")

    perf_metrics = PerfMetrics()
    _set_task_progress_msg(task, f"Starting async portion of task")

    addr = decode_surf_addr_str(surf_addr_str)
    if addr.address_type != "REAL":
        raise ValueError(f"Unsupported address type: {addr.address_type}")

    async with start_otel_span_async("get-xtgeo-surf"):
        _set_task_progress_msg(task, "Downloading and decoding surface data")
        access = SurfaceAccess.from_iteration_name(sumo_access_token, addr.case_uuid, addr.ensemble_name)
        xtgeo_surf: xtgeo.RegularSurface = await access.get_realization_surface_data_async(
            real_num=addr.realization,
            name=addr.name,
            attribute=addr.attribute,
            time_or_interval_str=addr.iso_time_or_interval,
        )
    perf_metrics.record_lap("get-xtgeo")

    if xtgeo_surf is None:
        logger.error(f"Failed to get xtgeo surface for {surf_addr_str=}")
        # We need something better here for these expected errors
        # We probably want to return a structured result from the task and have expected errors there
        return None

    if addr.realization % 2 != 0:
        raise ValueError("FAKE error on odd realizations")

    temp_user_store = get_temp_user_store_for_user_id(user_id)

    async with start_otel_span_async("store-result"):
        _set_task_progress_msg(task, "Storing result")
        surf_data_response = to_api_surface_data_float(xtgeo_surf)
        await temp_user_store.put_pydantic_model(target_store_key, surf_data_response, "msgpack", "surf-data-from-celery")
    perf_metrics.record_lap("store-result")

    _set_task_progress_msg(task, f"Finished async portion of task")

    logger.debug(f"do_surface_from_addr_async() took: {perf_metrics.to_string()}")

    return target_store_key


@celery_app.task(bind=True)
def surface_from_addr(self: Task, user_id: str, sumo_access_token: str, surf_addr_str: str, target_store_key: str) -> str | None:
    logger = logging.getLogger(__name__)
    logger.debug(f"surface_from_addr() --- : {surf_addr_str=}")

    perf_metrics = PerfMetrics()

    coro = do_surface_from_addr_async(
        self,
        user_id=user_id,
        sumo_access_token=sumo_access_token,
        surf_addr_str=surf_addr_str,
        target_store_key=target_store_key,
    )

    store_key = asyncio.run(coro)

    logger.debug(f"surface_from_addr() done in: {perf_metrics.to_string()}")

    return store_key
