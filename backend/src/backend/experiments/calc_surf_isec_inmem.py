import os
import signal
import numpy as np
import logging
from typing import List
import multiprocessing
import xtgeo
import asyncio
import os

# from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass

from src.services.sumo_access.surface_access import SurfaceAccess
from src.services.utils.authenticated_user import AuthenticatedUser
from src.backend.primary.routers.surface import schemas
from src.backend.utils.perf_metrics import PerfMetrics
from src.backend.experiments.caching import get_user_cache

LOGGER = logging.getLogger(__name__)


@dataclass
class SurfCacheEntry:
    surf: xtgeo.RegularSurface | None

class InMemSurfCache:
    def __init__(self):
        self._dict = {}

    def set(self, case_uuid: str, ensemble_name: str, name: str, attribute: str, real: int, cache_entry: SurfCacheEntry):
        key = f"{case_uuid}:{ensemble_name}:{name}:{attribute}:{real}"
        self._dict[key] = cache_entry

    def get(self, case_uuid: str, ensemble_name: str, name: str, attribute: str, real: int) -> SurfCacheEntry | None:
        key = f"{case_uuid}:{ensemble_name}:{name}:{attribute}:{real}"
        surf = self._dict.get(key)
        return surf


class RedisSurfCache:
    def __init__(self, authenticated_user: AuthenticatedUser):
        self._user_cache = get_user_cache(authenticated_user)

    async def set(self, case_uuid: str, ensemble_name: str, name: str, attribute: str, real: int, cache_entry: SurfCacheEntry):
        cache_key = f"surface:{case_uuid}_{ensemble_name}_Real{real}_{name}_{attribute}"
        xtgeo_surf = cache_entry.surf
        if xtgeo_surf is None:
            xtgeo_surf = xtgeo.RegularSurface(1, 1, 1, 1)
        await self._user_cache.set_RegularSurface_HACK(cache_key, xtgeo_surf)

    async def get(self, case_uuid: str, ensemble_name: str, name: str, attribute: str, real: int) -> SurfCacheEntry | None:
        cache_key = f"surface:{case_uuid}_{ensemble_name}_Real{real}_{name}_{attribute}"
        xtgeo_surf = await self._user_cache.get_RegularSurface_HACK(cache_key)
        if xtgeo_surf is None:
            return None

        if xtgeo_surf.ncol == 1 and xtgeo_surf.nrow == 1:
            return SurfCacheEntry(surf=None)
        
        return SurfCacheEntry(surf=xtgeo_surf)


IN_MEM_SURF_CACHE = InMemSurfCache()


@dataclass
class SurfItem:
    case_uuid: str
    ensemble_name: str
    name: str
    attribute: str
    real: int


global_access = None
global_many_surfs_getter = None


def init_access(access_token: str, case_uuid: str, ensemble_name: str, name: str, attribute: str):
    # !!!!!!!!!!!!!
    # See: https://github.com/tiangolo/fastapi/issues/1487#issuecomment-1157066306
    signal.set_wakeup_fd(-1)
    signal.signal(signal.SIGTERM, signal.SIG_DFL)
    signal.signal(signal.SIGINT, signal.SIG_DFL)

    global global_access
    global_access = SurfaceAccess.from_case_uuid_sync(access_token, case_uuid, ensemble_name)

    global global_many_surfs_getter
    global_many_surfs_getter = global_access.prepare_for_getting_many_realizations(name=name, attribute=attribute)


def fetch_a_surf(item: SurfItem) -> bytes:
    print(f">>>> fetch_a_surf {item.real=}", flush=True)

    # surf_bytes = global_access.get_realization_surface_bytes_sync(real_num=item.real, name=item.name, attribute=item.attribute)
    # if surf_bytes is None:
    #     return None

    # xtgeo_surf = global_access.get_realization_surface_data_sync(real_num=item.real, name=item.name, attribute=item.attribute)
    # if xtgeo_surf is None:
    #     return None

    xtgeo_surf = global_many_surfs_getter.get_real(real_num=item.real)
    if xtgeo_surf is None:
        return None

    print(f">>>> fetch_a_surf {item.real=} done", flush=True)

    #return surf_bytes
    return xtgeo_surf



async def calc_surf_isec_inmem(
    perf_metrics: PerfMetrics,
    authenticated_user: AuthenticatedUser,
    case_uuid: str,
    ensemble_name: str,
    name: str,
    attribute: str,
    num_reals: int,
    cutting_plane: schemas.CuttingPlane,
) -> List[schemas.SurfaceIntersectionData]:
    myprefix = ">>>>>>>>>>>>>>>>> calc_surf_isec_inmem():"
    print(f"{myprefix} started", flush=True)

    print(f"{myprefix}  {os.cpu_count()=}", flush=True)
    print(f"{myprefix}  {len(os.sched_getaffinity(0))=}", flush=True)
    print(f"{myprefix}  {query_cpu()=}", flush=True)


    fence_arr = np.array(
        [cutting_plane.x_arr, cutting_plane.y_arr, np.zeros(len(cutting_plane.y_arr)), cutting_plane.length_arr]
    ).T

    access_token = authenticated_user.get_sumo_access_token()

    reals = range(0, num_reals)

    xtgeo_surf_arr = []
    items_to_fetch_list = []

    redis_surf_cache = RedisSurfCache(authenticated_user)

    coro_arr = []
    for real in reals:
        coro_arr.append(redis_surf_cache.get(case_uuid, ensemble_name, name, attribute, real))
    print(f"{myprefix} - awaiting cache_entry_arr", flush=True)
    cache_entry_arr: List[SurfCacheEntry | None] = await asyncio.gather(*coro_arr)
    print(f"{myprefix} - resolved cache_entry_arr", flush=True)

    for real in reals:
        #cache_entry = IN_MEM_SURF_CACHE.get(case_uuid, ensemble_name, name, attribute, real)
        #cache_entry = await redis_surf_cache.get(case_uuid, ensemble_name, name, attribute, real)
        cache_entry = cache_entry_arr[real]
        if cache_entry is not None:
            xtgeo_surf_arr.append(cache_entry.surf)
        else:
            items_to_fetch_list.append(
                SurfItem(
                    case_uuid=case_uuid,
                    ensemble_name=ensemble_name,
                    name=name,
                    attribute=attribute,
                    real=real,
                )
            )

    print(f"{myprefix} {len(xtgeo_surf_arr)=}", flush=True)
    print(f"{myprefix} {len(items_to_fetch_list)=}", flush=True)

    if len(items_to_fetch_list) > 0:
        context = multiprocessing.get_context("spawn")
        with context.Pool(initializer=init_access, initargs=(access_token, case_uuid, ensemble_name, name, attribute)) as pool:
        #with multiprocessing.Pool(initializer=init_access, initargs=(access_token, case_uuid, ensemble_name, name, attribute)) as pool:
            print(f"{myprefix} just before map", flush=True)
            res_item_arr = pool.map(fetch_a_surf, items_to_fetch_list)
            print(f"{myprefix} back from map {len(res_item_arr)=}", flush=True)

            for idx, res_item in enumerate(res_item_arr):
                xtgeo_surf = None
                if res_item is not None:
                    print(f"{myprefix} {type(res_item)=}", flush=True)
                    xtgeo_surf = res_item
                    # byte_stream = io.BytesIO(res_item)
                    # xtgeo_surf = xtgeo.surface_from_file(byte_stream)

                xtgeo_surf_arr.append(xtgeo_surf)
                #IN_MEM_SURF_CACHE.set(case_uuid, ensemble_name, items_to_fetch_list[idx].name, items_to_fetch_list[idx].attribute, items_to_fetch_list[idx].real, cache_entry=SurfCacheEntry(xtgeo_surf))
                await redis_surf_cache.set(case_uuid, ensemble_name, items_to_fetch_list[idx].name, items_to_fetch_list[idx].attribute, items_to_fetch_list[idx].real, cache_entry=SurfCacheEntry(xtgeo_surf))

    intersections = []

    for xtgeo_surf in xtgeo_surf_arr:
        if (xtgeo_surf):
            line = xtgeo_surf.get_randomline(fence_arr)
            intersections.append(schemas.SurfaceIntersectionData(name="someName", hlen_arr=line[:, 0].tolist(), z_arr=line[:, 1].tolist()))

    return intersections


def query_cpu():
    cpu_quota = -1
    avail_cpu = -1

    if os.path.isfile('/sys/fs/cgroup/cpu/cpu.cfs_quota_us'):
        cpu_quota = int(open('/sys/fs/cgroup/cpu/cpu.cfs_quota_us').read().rstrip())
        print(cpu_quota) # Not useful for AWS Batch based jobs as result is -1, but works on local linux systems
    
    if cpu_quota != -1 and os.path.isfile('/sys/fs/cgroup/cpu/cpu.cfs_period_us'):
        cpu_period = int(open('/sys/fs/cgroup/cpu/cpu.cfs_period_us').read().rstrip())
        print(cpu_period)
        avail_cpu = int(cpu_quota / cpu_period) # Divide quota by period and you should get num of allotted CPU to the container, rounded down if fractional.
    elif os.path.isfile('/sys/fs/cgroup/cpu/cpu.shares'):
        cpu_shares = int(open('/sys/fs/cgroup/cpu/cpu.shares').read().rstrip())
        print(cpu_shares) # For AWS, gives correct value * 1024.
        avail_cpu = int(cpu_shares / 1024)
    
    return avail_cpu