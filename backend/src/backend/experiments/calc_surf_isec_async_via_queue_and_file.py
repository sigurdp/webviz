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
import io

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
    download_ok: bool
    real: int
    name: str
    attribute: str
    xtgeo_bytes: bytes


@dataclass
class ConvertedSurfItem:
    real: int
    name: str
    attribute: str
    quick_bytes: bytes


async def download_surf_to_queue(many_surfs_getter: ManyRealSurfsGetter, real: int, name: str, attribute: str, out_queue = multiprocessing.Queue()) -> DownloadedSurfItem:
    print(f">>>> download_surf_to_queue {real=}", flush=True)
    perf_metrics = PerfMetrics()

    surf_bytes = await many_surfs_getter.get_real_bytes_async(real_num=real)
    perf_metrics.record_lap("fetch")

    if surf_bytes is None:
        print(f">>>> download_surf_to_queue {real=} failed", flush=True)
        return DownloadedSurfItem(download_ok=False, real=real, name=name, attribute=attribute, xtgeo_bytes=None)

    item = DownloadedSurfItem(download_ok=True, real=real, name=name, attribute=attribute, xtgeo_bytes=surf_bytes)
    out_queue.put(item)
    perf_metrics.record_lap("enqueue")

    print(f">>>> download_surf_to_queue {real=} done in {perf_metrics.to_string()}", flush=True)

    return item


async def load_quick_surf(quick_file_name) -> xtgeo.RegularSurface | None:
    perf_metrics = PerfMetrics()

    try:
        async with aiofiles.open(quick_file_name, mode='rb') as f:
            my_bytes = await f.read()
            perf_metrics.record_lap("load")
    except:
        print(f">>>> load_quick_surf  failed to load {quick_file_name=}", flush=True)
        return None
        
    xtgeo_surf = bytes_to_xtgeo_surf(my_bytes)
    perf_metrics.record_lap("construct")

    print(f">>>> load_quick_surf  loaded {quick_file_name=} in {perf_metrics.to_string()}", flush=True)

    return xtgeo_surf


def convert_irap_to_quicksurf_worker(workerName: str, scratch_dir: str, in_queue: multiprocessing.Queue, out_queue: multiprocessing.Queue):
    while True:
        print(f"---- Worker {workerName} waiting for work...", flush=True)
        in_item: DownloadedSurfItem = in_queue.get()
        if in_item is None:
            print(f"---- Worker {workerName} exiting", flush=True)
            out_queue.put(None)
            return

        if not in_item.download_ok:
            print(f"---- Worker {workerName} download failed, skipping", flush=True)
            continue

        print(f"---- Worker {workerName} Doing work...", flush=True)

        perf_metrics = PerfMetrics()

        byte_stream = io.BytesIO(in_item.xtgeo_bytes)
        xtgeo_surf = xtgeo.surface_from_file(byte_stream)
        perf_metrics.record_lap("xtgeo-parse")

        my_bytes = xtgeo_surf_to_bytes(xtgeo_surf)

        out_item = ConvertedSurfItem(real=in_item.real, name=in_item.name, attribute=in_item.attribute, quick_bytes=my_bytes)
        #out_item = ConvertedSurfItem(real=in_item.real, name=in_item.name, attribute=in_item.attribute, quick_bytes=None)
        out_queue.put(out_item)

        quick_file_name = make_quicksurf_fn(scratch_dir, in_item.real, in_item.name, in_item.attribute)
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


def make_quicksurf_fn(scratch_dir: str, real: int, name: str, attribute: str) -> str:
    return f"{scratch_dir}/{name}-{attribute}-{real}.quick"



# ==========================================================
async def calc_surf_isec_async_via_queue_and_file(
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
    
    myprefix = ">>>>>>>>>>>>>>>>> calc_surf_isec_async_via_queue_and_file():"

    print(f"{myprefix} started #################################################################################################################", flush=True)
    print(f"{myprefix} started  {num_reals=}  {num_workers=}", flush=True)

    user_id = authenticated_user.get_user_id()
    access_token = authenticated_user.get_sumo_access_token()

    user_scratch_dir = f"/tmp/webvizcache/my_scratch/user_{user_id}/{case_uuid}__{ensemble_name}"
    os.makedirs(user_scratch_dir, exist_ok=True)
    print(f"{myprefix}  {user_scratch_dir=}", flush=True)


    # print(f"{myprefix}  ---------------------------", flush=True)
    # try:
    #     print(f"{myprefix}  {os.getcwd()=}", flush=True)
    #     print(f"{myprefix}  {os.listdir('.')=}", flush=True)
    #     print(f"{myprefix}  {os.listdir(my_scratch_dir)=}", flush=True)
    #     print(f"{myprefix}  {os.listdir('/tmp')=}", flush=True)
    #     print(f"{myprefix}  {os.listdir('/tmp/webvizcache')=}", flush=True)
    # except:
    #     pass
    # print(f"{myprefix}  ---------------------------", flush=True)


    fence_arr = np.array([cutting_plane.x_arr, cutting_plane.y_arr, np.zeros(len(cutting_plane.y_arr)), cutting_plane.length_arr]).T

    perf_metrics.record_lap("startup")

    access = await SurfaceAccess.from_case_uuid(access_token, case_uuid, ensemble_name)
    many_surfs_getter = access.prepare_for_getting_many_realizations(name=name, attribute=attribute)
    perf_metrics.record_lap("access")

    all_reals_to_get = range(0, num_reals)

    coro_arr = []
    for real in all_reals_to_get:
        quick_file_name = make_quicksurf_fn(user_scratch_dir, real, name, attribute)
        coro_arr.append(load_quick_surf(quick_file_name))

    xtgeo_surf_arr = []
    reals_to_download = []

    res_arr: List[xtgeo.RegularSurface | None] = await asyncio.gather(*coro_arr)
    for idx, may_xtgeo_surf in enumerate(res_arr):
        if may_xtgeo_surf:
            xtgeo_surf_arr.append(may_xtgeo_surf)
        else:
            reals_to_download.append(all_reals_to_get[idx])


    perf_metrics.record_lap("load-cached-quick")
    print(f"{myprefix}  {len(xtgeo_surf_arr)=} {len(reals_to_download)=}", flush=True)

    if len(reals_to_download) > 0:
        downloaded_queue = multiprocessing.Queue()
        converted_queue = multiprocessing.Queue()
        num_procs = 4
        proc_arr = []
        for proc_num in range(num_procs):
            p = multiprocessing.Process(target=convert_irap_to_quicksurf_worker, args=(f"worker_{proc_num}", user_scratch_dir, downloaded_queue, converted_queue))
            p.start()
            proc_arr.append(p)  

        perf_metrics.record_lap("start-procs")


        no_concurrent = num_workers
        dltasks = set()
        done_arr = []
        for real in reals_to_download:
            if len(dltasks) >= no_concurrent:
                # Wait for some download to finish before adding a new one
                donetasks, dltasks = await asyncio.wait(dltasks, return_when=asyncio.FIRST_COMPLETED)
                done_arr.extend(list(donetasks))

            dltasks.add(asyncio.create_task(download_surf_to_queue(many_surfs_getter, real, name, attribute, downloaded_queue)))
            

        # Wait for the remaining downloads to finish
        donetasks, _dummy = await asyncio.wait(dltasks)
        done_arr.extend(list(donetasks))

        perf_metrics.record_lap("download-to-disk")

        quick_file_names_to_load = []
        for donetask in done_arr:
            surf_item: DownloadedSurfItem = donetask.result()
            if surf_item.download_ok:
                quick_file_names_to_load.append(make_quicksurf_fn(user_scratch_dir, surf_item.real, surf_item.name, surf_item.attribute))

        for p in proc_arr:
            downloaded_queue.put(None)

        myTimer = PerfTimer()

        num_poison_pills = 0
        num_surfaces_loaded = 0
        while num_poison_pills < num_procs:
            # Should have anb async wrapper to the queue here since this call will block!!!!
            conv_item = converted_queue.get()
            if conv_item is None:
                num_poison_pills += 1
                continue

            if conv_item.quick_bytes is None:
                continue

            xtgeo_surf = bytes_to_xtgeo_surf(conv_item.quick_bytes)
            xtgeo_surf_arr.append(xtgeo_surf)
            num_surfaces_loaded += 1
        
        average_load_time_ms = myTimer.elapsed_ms()/num_surfaces_loaded if num_surfaces_loaded > 0 else 0
        print(f"{myprefix}  average quick surf load time for {num_surfaces_loaded} surfaces: {average_load_time_ms:.2f}ms", flush=True)

        # print(f"{myprefix}  downloaded_queue.close()", flush=True)
        # downloaded_queue.close()
        # print(f"{myprefix}  downloaded_queue.join_thread()", flush=True)
        # downloaded_queue.join_thread()

        # print(f"{myprefix}  converted_queue.close()", flush=True)
        # converted_queue.close()
        # print(f"{myprefix}  converted_queue.join_thread()", flush=True)
        # converted_queue.join_thread()

        print(f"{myprefix}  doing join on procs", flush=True)
        for idx, p in enumerate(proc_arr):
            print(f"{myprefix}  p.join()  {idx=}", flush=True)
            p.join()
        print(f"{myprefix}  join on procs finished", flush=True)

        perf_metrics.record_lap("wait-convert")


        """
        print(f"{myprefix}  {len(quick_file_names_to_load)=}", flush=True)

        myTimer = PerfTimer()

        if len(quick_file_names_to_load) > 0:
            num_surfaces_loaded = 0
            for quick_file_name in quick_file_names_to_load:
                if quick_file_name is not None:
                    xtgeo_surf = await load_quick_surf(quick_file_name)
                    if xtgeo_surf:
                        xtgeo_surf_arr.append(xtgeo_surf)
                        num_surfaces_loaded += 1

            average_load_time_ms = myTimer.elapsed_ms()/num_surfaces_loaded if num_surfaces_loaded > 0 else 0
            print(f"{myprefix}  average quick surf load time for {num_surfaces_loaded} surfaces: {average_load_time_ms:.2f}ms", flush=True)
            perf_metrics.record_lap("load-quick")
        """


    intersections = []
    for idx, xtgeo_surf in enumerate(xtgeo_surf_arr):
        if xtgeo_surf is not None:
            line = xtgeo_surf.get_randomline(fence_arr)
            intersections.append(schemas.SurfaceIntersectionData(name=f"{name}", hlen_arr=line[:, 0].tolist(), z_arr=line[:, 1].tolist()))

    perf_metrics.record_lap("cutting")

    print(f"{myprefix}  finished in {perf_metrics.to_string()}  ", flush=True)
    print(f"{myprefix}  finished #################################################################################################################", flush=True)

    return intersections
