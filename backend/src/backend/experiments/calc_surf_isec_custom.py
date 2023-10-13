import asyncio
import numpy as np
import logging
from typing import List
import multiprocessing
import xtgeo
from io import BytesIO

from src.services.sumo_access.surface_access import SurfaceAccess
from src.services.utils.authenticated_user import AuthenticatedUser
from src.backend.primary.routers.surface import schemas
from src.backend.utils.perf_metrics import PerfMetrics

LOGGER = logging.getLogger(__name__)


class AsyncQueue:
    SLEEP: float = 0.01

    def __init__(self, queue: multiprocessing.Queue):
        self._queue = queue

    async def get(self):
        while True:
            try:
                return self._queue.get_nowait()
            except multiprocessing.Empty:
                await asyncio.sleep(self.SLEEP)

    async def put(self, item):
        while True:
            try:
                self._queue.put_nowait(item)
                return None
            except multiprocessing.Full:
                await asyncio.sleep(self.SLEEP)



async def calc_surf_isec_custom(
    perf_metrics: PerfMetrics,
    authenticated_user: AuthenticatedUser,
    case_uuid: str,
    ensemble_name: str,
    name: str,
    attribute: str,
    num_reals: int,
    cutting_plane: schemas.CuttingPlane,
) -> List[schemas.SurfaceIntersectionData]:
    
    myprefix = ">>>>>>>>>>>>>>>>> calc_surf_isec_custom():"
    print(f"{myprefix} started with {num_reals=}", flush=True)

    perf_metrics.reset_lap_timer()

    access = await SurfaceAccess.from_case_uuid(authenticated_user.get_sumo_access_token(), case_uuid, ensemble_name)
    perf_metrics.record_lap("access")

    fence_arr = np.array([cutting_plane.x_arr, cutting_plane.y_arr, np.zeros(len(cutting_plane.y_arr)), cutting_plane.length_arr]).T

    reals = range(0, num_reals)

    multi_queue = multiprocessing.Queue()
    async_queue = AsyncQueue(multi_queue)

    num_procs=4
    proc_arr = []
    for proc_num in range(num_procs):
        p = multiprocessing.Process(target=process_surf_worker, args=(f"sigworker{proc_num}", multi_queue, fence_arr))
        p.start()
        proc_arr.append(p)  

    load_tasks = []
    for real in reals:
        task = asyncio.create_task(load_surf_bytes_task(async_queue, access, real, name, attribute))
        load_tasks.append(task)

    perf_metrics.record_lap("issue-tasks")

    await asyncio.gather(*load_tasks)

    for p in proc_arr:
        multi_queue.put(None)

    multi_queue.close()
    multi_queue.join_thread()

    for p in proc_arr:
        p.join()

    print(f"{myprefix} finished", flush=True)

    return []


async def load_surf_bytes_task(queue, access: SurfaceAccess, real_num: int, name: str, attribute: str):
    surf_bytes = await access.get_realization_surface_bytes_async(real_num=real_num, name=name, attribute=attribute)
    await queue.put(surf_bytes)


def process_surf_worker(workerName, queue, fence_arr):

    while True:
        print(f"Worker {workerName} waiting for work...", flush=True)
        theBytes = queue.get()
        if theBytes is None:
            print(f"Worker {workerName} exiting", flush=True)
            return

        print(f"Worker {workerName} Doing work...", flush=True)

        xtgeo_surf = xtgeo.surface_from_file(BytesIO(theBytes))

        size_mb = len(theBytes)/(1024*1024)
        nx = xtgeo_surf.ncol
        ny = xtgeo_surf.nrow

        print(f"Worker {workerName} read surf [{nx}x{ny}, {size_mb:.2f}MB]", flush=True)
