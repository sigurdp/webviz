import os
import signal
import numpy as np
import logging
from typing import List
import asyncio
import aiofiles
import xtgeo

# from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass

from src.services.sumo_access.surface_access import SurfaceAccess
from src.services.utils.authenticated_user import AuthenticatedUser
from src.backend.primary.routers.surface import schemas
from src.backend.utils.perf_metrics import PerfMetrics

LOGGER = logging.getLogger(__name__)


@dataclass
class DownloadedSurfItem:
    real: int
    surf_bytes: bytes
    file_name: str

async def download_surf_bytes(access: SurfaceAccess,  real: int, name: str, attribute: str, scratch_dir: str) -> DownloadedSurfItem:
    print(f">>>> download_surf_bytes {real=}", flush=True)
    perf_metrics = PerfMetrics()

    surf_bytes = await access.get_realization_surface_bytes_async(real_num=real, name=name, attribute=attribute)
    perf_metrics.record_lap("fetch")

    file_name = None
    if surf_bytes is not None:
        file_name = f"{scratch_dir}/{name}-{attribute}-{real}.bin"
        async with aiofiles.open(file_name, mode='wb') as f:
            await f.write(surf_bytes)
        perf_metrics.record_lap("write")

    print(f">>>> download_surf_bytes {real=} done  [{perf_metrics.to_string()}]", flush=True)

    return DownloadedSurfItem(real=real, surf_bytes=surf_bytes, file_name=file_name)


async def calc_surf_isec_async_via_file(
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
    
    myprefix = ">>>>>>>>>>>>>>>>> calc_surf_isec_async_via_file():"
    print(f"{myprefix} started  {num_reals=}  {num_workers=}", flush=True)

    my_scratch_dir = os.getcwd() + "/my_scratch"
    os.makedirs(my_scratch_dir, exist_ok=True)
    print(f"{myprefix}  {my_scratch_dir=}", flush=True)

    fence_arr = np.array([cutting_plane.x_arr, cutting_plane.y_arr, np.zeros(len(cutting_plane.y_arr)), cutting_plane.length_arr]).T

    perf_metrics.reset_lap_timer()

    access_token = authenticated_user.get_sumo_access_token()
    access = await SurfaceAccess.from_case_uuid(access_token, case_uuid, ensemble_name)
    perf_metrics.record_lap("access")

    reals = range(0, num_reals)

    no_concurrent = num_workers
    dltasks = set()
    done_arr = []
    for real in reals:
        if len(dltasks) >= no_concurrent:
            # Wait for some download to finish before adding a new one
            donetasks, dltasks = await asyncio.wait(dltasks, return_when=asyncio.FIRST_COMPLETED)
            done_arr.extend(list(donetasks))

        dltasks.add(asyncio.create_task(download_surf_bytes(access, real, name, attribute, my_scratch_dir)))
                    
    # Wait for the remaining downloads to finish
    donetasks, _dummy = await asyncio.wait(dltasks)
    done_arr.extend(list(donetasks))

    perf_metrics.record_lap("download")


    print(f"{myprefix}  {len(done_arr)=}", flush=True)

    xtgeo_surf_arr = []
    for task in done_arr:
        surf_item: DownloadedSurfItem = task.result()
        if surf_item.file_name is not None:
            xtgeo_surf = xtgeo.surface_from_file(surf_item.file_name)
            xtgeo_surf_arr.append(xtgeo_surf)
            print(f"{myprefix}  loaded xt_geo {surf_item.file_name=}", flush=True)

    perf_metrics.record_lap("load-xtgeo")


    intersections = []
    for idx, xtgeo_surf in enumerate(xtgeo_surf_arr):
        if xtgeo_surf is not None:
            line = xtgeo_surf.get_randomline(fence_arr)
            intersections.append(schemas.SurfaceIntersectionData(name=f"{name}", hlen_arr=line[:, 0].tolist(), z_arr=line[:, 1].tolist()))

    perf_metrics.record_lap("cutting")


    print(f"{myprefix}  finished in {perf_metrics.to_string()}", flush=True)

    return []
