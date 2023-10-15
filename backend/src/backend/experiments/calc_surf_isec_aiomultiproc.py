import signal
import numpy as np
import logging
import os
from typing import List
import aiomultiprocess

from dataclasses import dataclass

from src.services.sumo_access.surface_access import SurfaceAccess
from src.services.utils.authenticated_user import AuthenticatedUser
from src.backend.primary.routers.surface import schemas
from src.backend.utils.perf_metrics import PerfMetrics
from src.backend.experiments.caching import get_user_cache

LOGGER = logging.getLogger(__name__)

logging.getLogger("src.backend.experiments.caching").setLevel(level=logging.DEBUG)


aiomultiprocess.set_start_method("fork")



@dataclass
class SurfItem:
    case_uuid: str
    ensemble_name: str
    name: str
    attribute: str
    real: int


@dataclass
class ResultItem:
    perf_info: str
    line: np.ndarray


global_access = None
global_fence_arr = None
global_user_cache = None


def init_access_and_fence(authenticated_user: AuthenticatedUser, case_uuid: str, ensemble_name: str, fence_arr: np.ndarray):
    # !!!!!!!!!!!!!
    # See: https://github.com/tiangolo/fastapi/issues/1487#issuecomment-1157066306
    signal.set_wakeup_fd(-1)
    signal.signal(signal.SIGTERM, signal.SIG_DFL)
    signal.signal(signal.SIGINT, signal.SIG_DFL)

    access_token = authenticated_user.get_sumo_access_token()

    global global_access
    global global_fence_arr
    global global_user_cache
    global_access = SurfaceAccess.from_case_uuid_sync(access_token, case_uuid, ensemble_name)
    global_fence_arr = fence_arr
    global_user_cache = get_user_cache(authenticated_user)


async def process_a_surf(item: SurfItem) -> ResultItem:
    print(f">>>> process_a_surf {os.getpid()=} {item.real=}", flush=True)
    perf_metrics = PerfMetrics()

    cache_key = f"surface:{item.case_uuid}_{item.ensemble_name}_Real{item.real}_{item.name}_{item.attribute}"
    #cached_surf = await global_user_cache.get_RegularSurface(cache_key)
    cached_surf = await global_user_cache.get_RegularSurface_HACK(cache_key)
    perf_metrics.record_lap("read-cache")

    xtgeo_surf = cached_surf
    
    if xtgeo_surf is None:
        access = global_access

        xtgeo_surf = await access.get_realization_surface_data_async(real_num=item.real, name=item.name, attribute=item.attribute)
        if xtgeo_surf is None:
            return None
        perf_metrics.record_lap("fetch")

    line = xtgeo_surf.get_randomline(global_fence_arr)
    perf_metrics.record_lap("calc")

    res_item = ResultItem(perf_info=perf_metrics.to_string(), line=line)

    if cached_surf is None:
        #await global_user_cache.set_RegularSurface(cache_key, xtgeo_surf)
        await global_user_cache.set_RegularSurface_HACK(cache_key, xtgeo_surf)
        perf_metrics.record_lap("write-cache")

    print(f">>>> process_a_surf {os.getpid()=} {item.real=} done", flush=True)

    return res_item


async def calc_surf_isec_aiomultiproc(
    authenticated_user: AuthenticatedUser,
    case_uuid: str,
    ensemble_name: str,
    name: str,
    attribute: str,
    num_reals: int,
    cutting_plane: schemas.CuttingPlane,
) -> List[schemas.SurfaceIntersectionData]:
    
    myprefix = ">>>>>>>>>>>>>>>>> calc_surf_isec_aiomultiproc():"
    print(f"{myprefix} started", flush=True)

    fence_arr = np.array([cutting_plane.x_arr, cutting_plane.y_arr, np.zeros(len(cutting_plane.y_arr)), cutting_plane.length_arr]).T

    access_token = authenticated_user.get_sumo_access_token()

    item_list = []
    for i in range(num_reals):
        item_list.append(
            SurfItem(
                case_uuid=case_uuid,
                ensemble_name=ensemble_name,
                name=name,
                attribute=attribute,
                real=i,
            )
        )

    print(f"{myprefix} built item_list {len(item_list)=}", flush=True)

    # See
    # https://aiomultiprocess.omnilib.dev/en/latest/guide.html

    processes = os.cpu_count()
    childconcurrency = int(len(item_list)/processes)
    queuecount = int(processes/4)

    # processes = None # Defaults to CPU count
    # queuecount = None # Default is 1
    # childconcurrency = 4 # Default is 16

    async with aiomultiprocess.Pool(
        queuecount=queuecount,
        processes=processes,
        childconcurrency=childconcurrency,
        initializer=init_access_and_fence,
        initargs=[authenticated_user, case_uuid, ensemble_name, fence_arr],
    ) as pool:
        print(f"{myprefix} pool info {pool.process_count=}", flush=True)
        print(f"{myprefix} pool info {pool.queue_count=}", flush=True)
        print(f"{myprefix} pool info {pool.childconcurrency=}", flush=True)

        intersections = []
        async for res_item in pool.map(process_a_surf, item_list):
            if res_item is not None and res_item.line is not None:
                isecdata = schemas.SurfaceIntersectionData(
                    name="someName", hlen_arr=res_item.line[:, 0].tolist(), z_arr=res_item.line[:, 1].tolist()
                )
                intersections.append(isecdata)
                print(f"{myprefix} got isec {len(intersections)}  perf_info={res_item.perf_info}", flush=True)
            else:
                print(f"{myprefix} res_item is None", flush=True)

    print(f"{myprefix} finished", flush=True)

    return intersections
