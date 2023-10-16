import os
import signal
import numpy as np
import logging
from typing import List
import multiprocessing
import xtgeo
import io

# from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass

from src.services.sumo_access.surface_access import SurfaceAccess
from src.services.utils.authenticated_user import AuthenticatedUser
from src.backend.primary.routers.surface import schemas
from src.backend.utils.perf_metrics import PerfMetrics

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

    fence_arr = np.array(
        [cutting_plane.x_arr, cutting_plane.y_arr, np.zeros(len(cutting_plane.y_arr)), cutting_plane.length_arr]
    ).T

    access_token = authenticated_user.get_sumo_access_token()

    reals = range(0, num_reals)

    xtgeo_surf_arr = []
    items_to_fetch_list = []

    for real in reals:
        cache_entry = IN_MEM_SURF_CACHE.get(case_uuid, ensemble_name, name, attribute, real)
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
        with multiprocessing.Pool(initializer=init_access, initargs=(access_token, case_uuid, ensemble_name, name, attribute)) as pool:
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
                IN_MEM_SURF_CACHE.set(case_uuid, ensemble_name, items_to_fetch_list[idx].name, items_to_fetch_list[idx].attribute, items_to_fetch_list[idx].real, cache_entry=SurfCacheEntry(xtgeo_surf))

    intersections = []

    for xtgeo_surf in xtgeo_surf_arr:
        if (xtgeo_surf):
            line = xtgeo_surf.get_randomline(fence_arr)
            intersections.append(schemas.SurfaceIntersectionData(name="someName", hlen_arr=line[:, 0].tolist(), z_arr=line[:, 1].tolist()))

    return intersections
