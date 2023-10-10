import logging
from typing import List, Union, Optional
from typing import Annotated

import asyncio
import numpy as np
import xtgeo

from fastapi import APIRouter, Depends, HTTPException, Query, Body, Response, Request

from src.services.sumo_access.surface_access import SurfaceAccess
from src.services.smda_access.stratigraphy_access import StratigraphyAccess
from src.services.smda_access.stratigraphy_utils import sort_stratigraphic_names_by_hierarchy
from src.services.smda_access.mocked_drogon_smda_access import _mocked_stratigraphy_access
from src.services.utils.statistic_function import StatisticFunction
from src.services.utils.authenticated_user import AuthenticatedUser
from src.services.utils.perf_timer import PerfTimer
from src.backend.auth.auth_helper import AuthHelper
from src.backend.utils.perf_metrics import PerfMetrics
from src.services.sumo_access._helpers import SumoCase

from src.backend.primary.routers.surface import schemas



LOGGER = logging.getLogger(__name__)

router = APIRouter()

@router.post("/sigtest")
def post_sigtest(
    case_uuid: str = Query(description="Sumo case uuid"),
    ensemble_name: str = Query(description="Ensemble name"),
    name: str = Query(description="Surface names"),
    attribute: str = Query(description="Surface attribute"),
    cutting_plane: schemas.CuttingPlane = Body(embed=True),
) -> str:
    print("!!!!!!!!!!!!!!!!!!!!!! post_sigtest()")
    print(f"{case_uuid=}")
    print(f"{ensemble_name=}")
    print(f"{name=}")
    print(f"{attribute=}")

    print(f"{type(cutting_plane)=}")

    return "sig_test"


@router.post("/calc_surf_intersections_fetch_first")
async def post_calc_surf_intersections_fetch_first(
    authenticated_user: Annotated[AuthenticatedUser, Depends(AuthHelper.get_authenticated_user)],
    response: Response,
    case_uuid: str,
    ensemble_name: str,
    name: str,
    attribute: str,
    num_reals: int,
    cutting_plane: schemas.CuttingPlane = Body(embed=True),
) ->  List[schemas.SurfaceIntersectionData]:
    print("!!!!!!!!!!!!!!!!!!!!!! post_calc_surf_intersections_fetch_first() started")
    
    perf_metrics = PerfMetrics(response)
    access = await SurfaceAccess.from_case_uuid(authenticated_user.get_sumo_access_token(), case_uuid, ensemble_name)
    perf_metrics.record_lap("access")

    fence_arr = np.array(
        [cutting_plane.x_arr, cutting_plane.y_arr, np.zeros(len(cutting_plane.y_arr)), cutting_plane.length_arr]
    ).T

    LOGGER.debug(f"{num_reals=}")

    reals = range(0, num_reals)

    coro_arr = []
    real_arr = []
    for real in reals:
        real_arr.append(real)
        coro_arr.append(access.get_realization_surface_data(real_num=real, name=name, attribute=attribute))

    perf_metrics.record_lap("issue-requests")

    res_arr: List[xtgeo.RegularSurface | None] = await asyncio.gather(*coro_arr)
    perf_metrics.record_lap("wait-for-requests")

    intersections = []
    for idx, xtgeo_surf in enumerate(res_arr):
        if xtgeo_surf is not None:
            print(f"Cutting surface {name} with attribute {attribute}-{real_arr[idx]}")
            line = xtgeo_surf.get_randomline(fence_arr)
            intersections.append(
                schemas.SurfaceIntersectionData(name=f"{name}", hlen_arr=line[:, 0].tolist(), z_arr=line[:, 1].tolist())
            )
        else:
            print(f"Skipping surface {name} with attribute {attribute}-{real_arr[idx]}")    

    perf_metrics.record_lap("cutting")

    LOGGER.debug(f"Intersected {len(res_arr)} surfaces in: {perf_metrics.to_string()}")

    print("!!!!!!!!!!!!!!!!!!!!!! post_calc_surf_intersections_fetch_first() finished")

    return intersections


@router.post("/calc_surf_intersections_queue")
async def post_calc_surf_intersections_queue(
    authenticated_user: Annotated[AuthenticatedUser, Depends(AuthHelper.get_authenticated_user)],
    response: Response,
    case_uuid: str,
    ensemble_name: str,
    name: str,
    attribute: str,
    num_reals: int,
    num_workers: int,
    cutting_plane: schemas.CuttingPlane = Body(embed=True),
) ->  List[schemas.SurfaceIntersectionData]:
    
    print("!!!!!!!!!!!!!!!!!!!!!! post_calc_surf_intersections_queue() started")

    perf_metrics = PerfMetrics(response)
    access = await SurfaceAccess.from_case_uuid(authenticated_user.get_sumo_access_token(), case_uuid, ensemble_name)
    perf_metrics.record_lap("access")

    fence_arr = np.array(
        [cutting_plane.x_arr, cutting_plane.y_arr, np.zeros(len(cutting_plane.y_arr)), cutting_plane.length_arr]
    ).T

    LOGGER.debug(f"{num_reals=}  {num_workers=}")

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

    LOGGER.debug(f"Intersected {len(intersections)} surfaces in: {perf_metrics.to_string()}")

    print("!!!!!!!!!!!!!!!!!!!!!! post_calc_surf_intersections_queue() finished")

    return intersections


async def item_producer(queue, reals, name: str, attribute: str):
    for real in reals:
        await queue.put({"real_num": real, "name": name, "attribute": attribute})


async def worker(name, queue, access: SurfaceAccess, fence_arr: np.ndarray, intersections: List[schemas.SurfaceIntersectionData]):
    while not queue.empty():
        item = await queue.get()

        isect_data = await _process_one_surface(access, item["real_num"], item["name"], item["attribute"], fence_arr)
        if isect_data is not None:
            intersections.append(isect_data)

        # Notify the queue that the "work item" has been processed.
        queue.task_done()


async def _process_one_surface(access: SurfaceAccess, real_num: int, name: str, attribute: str, fence_arr: np.ndarray) -> schemas.SurfaceIntersectionData | None:

    print(f"Downloading surface {name} with attribute {attribute}-{real_num}")
    xtgeo_surf = await access.get_realization_surface_data(real_num=real_num, name=name, attribute=attribute)
    if xtgeo_surf is None:
        print(f"Skipping surface {name} with attribute {attribute}-{real_num}")
        return None
    
    print(f"Cutting surface {name} with attribute {attribute}-{real_num}")
    line = xtgeo_surf.get_randomline(fence_arr)
    
    return schemas.SurfaceIntersectionData(name=f"{name}", hlen_arr=line[:, 0].tolist(), z_arr=line[:, 1].tolist())
