import asyncio
import numpy as np
import logging
from typing import List

from src.services.sumo_access.surface_access import SurfaceAccess
from src.services.utils.authenticated_user import AuthenticatedUser
from src.backend.primary.routers.surface import schemas
from src.backend.utils.perf_metrics import PerfMetrics

LOGGER = logging.getLogger(__name__)


async def calc_surf_isec_queue(
    perf_metrics: PerfMetrics,
    authenticated_user: AuthenticatedUser,
    case_uuid: str,
    ensemble_name: str,
    name: str,
    attribute: str,
    num_reals: int,
    num_workers: int,
    cutting_plane: schemas.CuttingPlane,
) -> List[schemas.SurfaceIntersectionData]:
    
    myprefix = ">>>>>>>>>>>>>>>>> calc_surf_isec_queue():"
    print(f"{myprefix} started with {num_reals=}  {num_workers=}")

    perf_metrics.reset_lap_timer()

    access = await SurfaceAccess.from_case_uuid(authenticated_user.get_sumo_access_token(), case_uuid, ensemble_name)
    perf_metrics.record_lap("access")

    fence_arr = np.array(
        [cutting_plane.x_arr, cutting_plane.y_arr, np.zeros(len(cutting_plane.y_arr)), cutting_plane.length_arr]
    ).T

    reals = range(0, num_reals)

    queue = asyncio.Queue()
    intersections = []

    producer_task = asyncio.create_task(item_producer(queue, reals, name, attribute))

    worker_tasks = []
    for i in range(num_workers):
        task = asyncio.create_task(worker(f"worker-{i}", queue, access, fence_arr, intersections))
        worker_tasks.append(task)

    perf_metrics.record_lap("setup")

    await asyncio.gather(producer_task, *worker_tasks)
    perf_metrics.record_lap("cutting")

    print(f"{myprefix} finished")

    return intersections


async def item_producer(queue, reals, name: str, attribute: str):
    for real in reals:
        await queue.put({"real_num": real, "name": name, "attribute": attribute})


async def worker(name, queue, access: SurfaceAccess, fence_arr: np.ndarray, intersections: List[schemas.SurfaceIntersectionData]):
    while not queue.empty():
        item = await queue.get()

        isect_data = await process_one_surface(access, item["real_num"], item["name"], item["attribute"], fence_arr)
        if isect_data is not None:
            intersections.append(isect_data)

        # Notify the queue that the "work item" has been processed.
        queue.task_done()


async def process_one_surface(access: SurfaceAccess, real_num: int, name: str, attribute: str, fence_arr: np.ndarray) -> schemas.SurfaceIntersectionData | None:

    print(f"Downloading surface {name} with attribute {attribute}-{real_num}")
    xtgeo_surf = await access.get_realization_surface_data_async(real_num=real_num, name=name, attribute=attribute)
    if xtgeo_surf is None:
        print(f"Skipping surface {name} with attribute {attribute}-{real_num}")
        return None
    
    print(f"Cutting surface {name} with attribute {attribute}-{real_num}")
    line = xtgeo_surf.get_randomline(fence_arr)
    
    return schemas.SurfaceIntersectionData(name=f"{name}", hlen_arr=line[:, 0].tolist(), z_arr=line[:, 1].tolist())
