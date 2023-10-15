import logging
import os
from typing import List
from typing import Annotated

from fastapi import APIRouter, Depends, Query, Body, Response

from src.services.utils.authenticated_user import AuthenticatedUser
from src.backend.auth.auth_helper import AuthHelper
from src.backend.utils.perf_metrics import PerfMetrics

from src.backend.primary.routers.surface import schemas

from src.backend.experiments.calc_surf_isec_fetch_first import calc_surf_isec_fetch_first
from src.backend.experiments.calc_surf_isec_queue import calc_surf_isec_queue
from src.backend.experiments.calc_surf_isec_multiprocess import calc_surf_isec_multiprocess
from src.backend.experiments.calc_surf_isec_aiomultiproc import calc_surf_isec_aiomultiproc
from src.backend.experiments.calc_surf_isec_custom import calc_surf_isec_custom


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


@router.post("/calc_surf_isec_experiments")
async def post_calc_surf_isec_experiments(
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

    LOGGER.debug(f"route calc_surf_isec_experiments - entering  actual {os.cpu_count()=}")

    perf_metrics = PerfMetrics(response)

    #intersections = await calc_surf_isec_fetch_first(perf_metrics, authenticated_user, case_uuid, ensemble_name, name, attribute, num_reals, cutting_plane)
    #intersections = await calc_surf_isec_queue(perf_metrics, authenticated_user, case_uuid, ensemble_name, name, attribute, num_reals, num_workers, cutting_plane)
    #intersections = await calc_surf_isec_multiprocess(perf_metrics, authenticated_user, case_uuid, ensemble_name, name, attribute, num_reals, cutting_plane)
    intersections = await calc_surf_isec_aiomultiproc(authenticated_user, case_uuid, ensemble_name, name, attribute, num_reals, cutting_plane)
    #intersections = await calc_surf_isec_custom(perf_metrics, authenticated_user, case_uuid, ensemble_name, name, attribute, num_reals, num_workers, cutting_plane)

    LOGGER.debug(f"route calc_surf_isec_experiments - intersected {len(intersections)} surfaces in: {perf_metrics.to_string()}")

    return intersections

