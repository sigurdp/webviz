import os
import signal
import numpy as np
import logging
from typing import List
import asyncio
import aiofiles
import xtgeo
import struct
import multiprocessing

# from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass

from src.services.sumo_access.surface_access import SurfaceAccess
from src.services.sumo_access.surface_access import ManyRealSurfsGetter
from src.services.utils.authenticated_user import AuthenticatedUser
from src.backend.primary.routers.surface import schemas
from src.backend.utils.perf_metrics import PerfMetrics
from src.services.utils.perf_timer import PerfTimer

LOGGER = logging.getLogger(__name__)


@dataclass
class DownloadedSurfItem:
    real: int
    base_file_name: str

async def download_surf_to_disk_using_access(access: SurfaceAccess,  real: int, name: str, attribute: str, scratch_dir: str, out_queue = multiprocessing.Queue()) -> DownloadedSurfItem:
    print(f">>>> download_surf_to_disk {real=}", flush=True)
    perf_metrics = PerfMetrics()

    surf_bytes = await access.get_realization_surface_bytes_async(real_num=real, name=name, attribute=attribute)
    perf_metrics.record_lap("fetch")

    if surf_bytes is None:
        print(f">>>> download_surf_to_disk {real=} failed", flush=True)
        return DownloadedSurfItem(real=real, base_file_name=None)

    base_file_name = f"{scratch_dir}/{name}-{attribute}-{real}"
    irap_file_name = base_file_name + ".bin"
    async with aiofiles.open(irap_file_name, mode='wb') as f:
        await f.write(surf_bytes)

    perf_metrics.record_lap("write")
    out_queue.put(base_file_name)

    print(f">>>> download_surf_to_disk {real=} done in {perf_metrics.to_string()}", flush=True)

    return DownloadedSurfItem(real=real, base_file_name=base_file_name)


async def download_surf_to_disk_using_many_surf_getter(many_surfs_getter: ManyRealSurfsGetter,  real: int, name: str, attribute: str, scratch_dir: str, out_queue = multiprocessing.Queue()) -> DownloadedSurfItem:
    print(f">>>> download_surf_to_disk {real=}", flush=True)
    perf_metrics = PerfMetrics()

    surf_bytes = await many_surfs_getter.get_real_bytes_async(real_num=real)
    perf_metrics.record_lap("fetch")

    if surf_bytes is None:
        print(f">>>> download_surf_to_disk {real=} failed", flush=True)
        return DownloadedSurfItem(real=real, base_file_name=None)

    base_file_name = f"{scratch_dir}/{name}-{attribute}-{real}"
    irap_file_name = base_file_name + ".bin"
    async with aiofiles.open(irap_file_name, mode='wb') as f:
        await f.write(surf_bytes)

    perf_metrics.record_lap("write")
    out_queue.put(base_file_name)

    print(f">>>> download_surf_to_disk {real=} done in {perf_metrics.to_string()}", flush=True)

    return DownloadedSurfItem(real=real, base_file_name=base_file_name)


async def load_quick_surf(quick_file_name) -> xtgeo.RegularSurface:
    perf_metrics = PerfMetrics()

    async with aiofiles.open(quick_file_name, mode='rb') as f:
        my_bytes = await f.read()
        perf_metrics.record_lap("load")
        
    xtgeo_surf = bytes_to_xtgeo_surf(my_bytes)
    perf_metrics.record_lap("construct")

    print(f">>>> load_quick_surf  loaded {quick_file_name=} in {perf_metrics.to_string()}", flush=True)

    return xtgeo_surf


def convert_irap_to_quick_format_process_worker(workerName, queue: multiprocessing.Queue):
    while True:
        print(f"---- Worker {workerName} waiting for work...", flush=True)
        base_file_name = queue.get()
        if base_file_name is None:
            print(f"---- Worker {workerName} exiting", flush=True)
            return

        print(f"---- Worker {workerName} Doing work...", flush=True)

        irap_file_name = base_file_name + ".bin"
        quick_file_name = base_file_name + ".quick"

        perf_metrics = PerfMetrics()

        xtgeo_surf = xtgeo.surface_from_file(irap_file_name)
        perf_metrics.record_lap("xtgeo-read")

        my_bytes = xtgeo_surf_to_bytes(xtgeo_surf)
        with open(quick_file_name, mode='wb') as f:
            f.write(my_bytes)
        perf_metrics.record_lap("write-quick")

        print(f"---- Worker converted {quick_file_name=} in {perf_metrics.to_string()}", flush=True)


def xtgeo_surf_to_bytes(surf: xtgeo.RegularSurface) -> bytes:
    header_bytes = struct.pack("@iiddddid", surf.ncol, surf.nrow, surf.xinc, surf.yinc, surf.xori, surf.yori, surf.yflip, surf.rotation)

    masked_values = surf.values.astype(np.float32)
    values_np = np.ma.filled(masked_values, fill_value=np.nan)
    arr_bytes = bytes(values_np.ravel(order="C").data)

    ret_arr = header_bytes + arr_bytes
    return ret_arr


def bytes_to_xtgeo_surf(byte_arr: bytes) -> xtgeo.RegularSurface:
    # 3*4 + 5*8 = 52
    ncol, nrow, xinc, yinc, xori, yori, yflip, rotation = struct.unpack("iiddddid", byte_arr[:56])
    values = np.frombuffer(byte_arr[56:], dtype=np.float32).reshape(nrow, ncol)
    surf = xtgeo.RegularSurface(
        ncol=ncol,
        nrow=nrow,
        xinc=xinc,
        yinc=yinc,
        xori=xori,
        yori=yori,
        yflip=yflip,
        rotation=rotation,
        values=values,
    )

    return surf


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
    many_surfs_getter = access.prepare_for_getting_many_realizations(name=name, attribute=attribute)
    perf_metrics.record_lap("access")

    reals = range(0, num_reals)


    multi_queue = multiprocessing.Queue()
    num_procs = 4
    proc_arr = []
    for proc_num in range(num_procs):
        p = multiprocessing.Process(target=convert_irap_to_quick_format_process_worker, args=(f"worker_{proc_num}", multi_queue))
        p.start()
        proc_arr.append(p)  

    perf_metrics.record_lap("start-procs")


    no_concurrent = num_workers
    dltasks = set()
    done_arr = []
    for real in reals:
        if len(dltasks) >= no_concurrent:
            # Wait for some download to finish before adding a new one
            donetasks, dltasks = await asyncio.wait(dltasks, return_when=asyncio.FIRST_COMPLETED)
            done_arr.extend(list(donetasks))

        #dltasks.add(asyncio.create_task(download_surf_to_disk_using_access(access, real, name, attribute, my_scratch_dir, multi_queue)))
        dltasks.add(asyncio.create_task(download_surf_to_disk_using_many_surf_getter(many_surfs_getter, real, name, attribute, my_scratch_dir, multi_queue)))
        

    # Wait for the remaining downloads to finish
    donetasks, _dummy = await asyncio.wait(dltasks)
    done_arr.extend(list(donetasks))

    perf_metrics.record_lap("download-to-disk")

    quick_file_names_arr = []
    for donetask in done_arr:
        surf_item: DownloadedSurfItem = donetask.result()
        if surf_item.base_file_name is not None:
            quick_file_names_arr.append(surf_item.base_file_name + ".quick")
        else:
            quick_file_names_arr.append(None)

    for p in proc_arr:
        multi_queue.put(None)

    multi_queue.close()
    multi_queue.join_thread()
    for p in proc_arr:
        p.join()

    perf_metrics.record_lap("wait-convert")

    print(f"{myprefix}  {len(quick_file_names_arr)=}", flush=True)


    myTimer = PerfTimer()

    xtgeo_surf_arr = []
    for quick_file_name in quick_file_names_arr:
        if quick_file_name is not None:
            xtgeo_surf = await load_quick_surf(quick_file_name)
            xtgeo_surf_arr.append(xtgeo_surf)

    print(f"{myprefix}  average quick surf load time: {myTimer.elapsed_ms()/len(xtgeo_surf_arr):.2f}ms", flush=True)
    perf_metrics.record_lap("load-quick")


    intersections = []
    for idx, xtgeo_surf in enumerate(xtgeo_surf_arr):
        if xtgeo_surf is not None:
            line = xtgeo_surf.get_randomline(fence_arr)
            intersections.append(schemas.SurfaceIntersectionData(name=f"{name}", hlen_arr=line[:, 0].tolist(), z_arr=line[:, 1].tolist()))

    perf_metrics.record_lap("cutting")


    print(f"{myprefix}  finished in {perf_metrics.to_string()}", flush=True)

    return intersections
