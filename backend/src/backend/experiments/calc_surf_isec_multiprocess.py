import asyncio
import numpy as np
import logging
from typing import List, Union, Optional
from multiprocessing import Pool
#from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass

from src.services.sumo_access.surface_access import SurfaceAccess
from src.services.utils.authenticated_user import AuthenticatedUser
from src.backend.primary.routers.surface import schemas
from src.backend.utils.perf_metrics import PerfMetrics

LOGGER = logging.getLogger(__name__)




@dataclass
class SurfItem:
    access_token: str
    case_uuid: str
    ensemble_name: str
    name: str
    attribute: str
    real: int
    fence_arr: np.ndarray

@dataclass
class ResultItem:
    perf_info:  str
    line: np.ndarray


# global_access = None
# global_fence_arr = None


# def init_access_and_fence(access_token: str, case_uuid: str, ensemble_name: str, fence_arr: np.ndarray):
#     global global_access
#     global global_fence_arr
#     global_access = asyncio.run(SurfaceAccess.from_case_uuid(access_token, case_uuid, ensemble_name))
#     global_fence_arr = fence_arr


async def process_a_surf(item: SurfItem) -> ResultItem:
    print(f"fetch_a_surf {item.real=}")
    perf_metrics = PerfMetrics()

    #access = global_access
    access = await SurfaceAccess.from_case_uuid(item.access_token, item.case_uuid, item.ensemble_name)
    perf_metrics.record_lap("access")

    xtgeo_surf = await access.get_realization_surface_data(real_num=item.real, name=item.name, attribute=item.attribute)
    if xtgeo_surf is None:
        return None
    perf_metrics.record_lap("fetch")

    line = xtgeo_surf.get_randomline(item.fence_arr)
    perf_metrics.record_lap("calc")

    res_item = ResultItem(perf_info=perf_metrics.to_string(), line=line)
    return res_item


def process_a_surf_in_a_loop(item: SurfItem) -> ResultItem:
    return asyncio.run(process_a_surf(item))



async def calc_surf_isec_multiprocess(
    perf_metrics: PerfMetrics,
    authenticated_user: AuthenticatedUser,
    case_uuid: str,
    ensemble_name: str,
    name: str,
    attribute: str,
    num_reals: int,
    cutting_plane: schemas.CuttingPlane,
) -> List[schemas.SurfaceIntersectionData]:

    myprefix = ">>>>>>>>>>>>>>>>> calc_surf_isec_multiprocess():"
    print(f"{myprefix} started")

    fence_arr = np.array(
        [cutting_plane.x_arr, cutting_plane.y_arr, np.zeros(len(cutting_plane.y_arr)), cutting_plane.length_arr]
    ).T

    access_token=authenticated_user.get_sumo_access_token()

    item_list = []
    for i in range(num_reals):
        item_list.append(SurfItem(
            access_token=access_token,
            case_uuid=case_uuid,
            ensemble_name=ensemble_name,
            name=name,
            attribute=attribute,
            real=i,
            fence_arr=fence_arr
        ))

    print(f"{myprefix} built item_list {len(item_list)=}")

    intersections = []
    with Pool() as pool:
        res_item_arr = pool.map(process_a_surf_in_a_loop, item_list)
        print(f"{myprefix} back from map {len(res_item_arr)=}")

        for res_item in res_item_arr:
            if res_item is not None and res_item.line is not None:
                isecdata = schemas.SurfaceIntersectionData(name="someName", hlen_arr=res_item.line[:, 0].tolist(), z_arr=res_item.line[:, 1].tolist())
                intersections.append(isecdata)
                print(f"{myprefix} got isec {len(intersections)}  perf_info={res_item.perf_info}")
            else:
                print(f"{myprefix} res_item is None")

    print(f"{myprefix} finished")

    return intersections


