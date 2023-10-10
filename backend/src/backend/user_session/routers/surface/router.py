import logging
from typing import List, Union, Optional

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


@router.post("/calc_surf_intersections")
async def post_calc_surf_intersections(
    response: Response,
    authenticated_user: AuthenticatedUser = Depends(AuthHelper.get_authenticated_user),
    case_uuid: str = Query(description="Sumo case uuid"),
    ensemble_name: str = Query(description="Ensemble name"),
    name: str = Query(description="Surface names"),
    attribute: str = Query(description="Surface attribute"),
    num_reals: int = Query(description="Number of realizations to intersect"),
    num_workers: int = Query(description="Number of workers to use"),
    cutting_plane: schemas.CuttingPlane = Body(embed=True),
) ->  List[schemas.SurfaceIntersectionData]:
    print("!!!!!!!!!!!!!!!!!!!!!! post_calc_surf_intersections() started")
    
    perf_metrics = PerfMetrics(response)
    access = await SurfaceAccess.from_case_uuid(authenticated_user.get_sumo_access_token(), case_uuid, ensemble_name)
    perf_metrics.record_lap("access")

    fence_arr = np.array(
        [cutting_plane.x_arr, cutting_plane.y_arr, np.zeros(len(cutting_plane.y_arr)), cutting_plane.length_arr]
    ).T

    LOGGER.debug(f"{num_reals=}  {num_workers=}")

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

    print("!!!!!!!!!!!!!!!!!!!!!! post_calc_surf_intersections() finished")

    return intersections
