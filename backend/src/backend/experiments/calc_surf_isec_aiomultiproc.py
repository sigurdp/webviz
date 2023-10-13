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

LOGGER = logging.getLogger(__name__)

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


def init_access_and_fence(access_token: str, case_uuid: str, ensemble_name: str, fence_arr: np.ndarray):
    global global_access
    global global_fence_arr
    global_access = SurfaceAccess.from_case_uuid_sync(access_token, case_uuid, ensemble_name)
    global_fence_arr = fence_arr


async def process_a_surf(item: SurfItem) -> ResultItem:
    print(f">>>> process_a_surf {os.getpid()=} {item.real=}", flush=True)
    perf_metrics = PerfMetrics()

    access = global_access

    xtgeo_surf = await access.get_realization_surface_data_async(real_num=item.real, name=item.name, attribute=item.attribute)
    if xtgeo_surf is None:
        return None
    perf_metrics.record_lap("fetch")

    line = xtgeo_surf.get_randomline(global_fence_arr)
    perf_metrics.record_lap("calc")

    res_item = ResultItem(perf_info=perf_metrics.to_string(), line=line)

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

    processes = None # Defaults to CPU count
    queuecount = None # Default is 1
    childconcurrency = 4 # Default is 16

    async with aiomultiprocess.Pool(
        queuecount=queuecount,
        processes=processes,
        childconcurrency=childconcurrency,
        initializer=init_access_and_fence,
        initargs=[access_token, case_uuid, ensemble_name, fence_arr],
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
