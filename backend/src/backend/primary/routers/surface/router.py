import logging
from typing import List, Union, Optional
from dataclasses import dataclass

import asyncio
import asyncio
from aiomultiprocess import Pool
import httpx
import numpy as np
import xtgeo
import json
from pydantic import BaseModel

from fastapi import APIRouter, Depends, HTTPException, Query, Body, Response, Request
from src.backend.primary.user_session_proxy import proxy_to_user_session
from src.backend.primary.user_session_proxy import get_user_session_base_url

from src.backend.user_session.routers.grid.router import get_grid_geometry
from src.backend.user_session.routers.grid.router import get_grid_parameter


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


from . import converters
from . import schemas


LOGGER = logging.getLogger(__name__)

router = APIRouter()


@router.get("/surface_directory/")
async def get_surface_directory(
    authenticated_user: AuthenticatedUser = Depends(AuthHelper.get_authenticated_user),
    case_uuid: str = Query(description="Sumo case uuid"),
    ensemble_name: str = Query(description="Ensemble name"),
) -> List[schemas.SurfaceMeta]:
    """
    Get a directory of surfaces in a Sumo ensemble
    """
    surface_access = await SurfaceAccess.from_case_uuid(
        authenticated_user.get_sumo_access_token(), case_uuid, ensemble_name
    )
    sumo_surf_dir = await surface_access.get_surface_directory()

    case_inspector = await SumoCase.from_case_uuid(authenticated_user.get_sumo_access_token(), case_uuid)
    strat_column_identifier = await case_inspector.get_stratigraphic_column_identifier()
    strat_access: Union[StratigraphyAccess, _mocked_stratigraphy_access.StratigraphyAccess]

    if strat_column_identifier == "DROGON_HAS_NO_STRATCOLUMN":
        strat_access = _mocked_stratigraphy_access.StratigraphyAccess(authenticated_user.get_smda_access_token())
    else:
        strat_access = StratigraphyAccess(authenticated_user.get_smda_access_token())
    strat_units = await strat_access.get_stratigraphic_units(strat_column_identifier)
    sorted_stratigraphic_surfaces = sort_stratigraphic_names_by_hierarchy(strat_units)

    return converters.to_api_surface_directory(sumo_surf_dir, sorted_stratigraphic_surfaces)


@router.get("/realization_surface_data/")
async def get_realization_surface_data(
    response: Response,
    authenticated_user: AuthenticatedUser = Depends(AuthHelper.get_authenticated_user),
    case_uuid: str = Query(description="Sumo case uuid"),
    ensemble_name: str = Query(description="Ensemble name"),
    realization_num: int = Query(description="Realization number"),
    name: str = Query(description="Surface name"),
    attribute: str = Query(description="Surface attribute"),
    time_or_interval: Optional[str] = Query(None, description="Time point or time interval string"),
) -> schemas.SurfaceData:
    perf_metrics = PerfMetrics(response)

    access = await SurfaceAccess.from_case_uuid(authenticated_user.get_sumo_access_token(), case_uuid, ensemble_name)
    xtgeo_surf = await access.get_realization_surface_data(
        real_num=realization_num, name=name, attribute=attribute, time_or_interval_str=time_or_interval
    )
    perf_metrics.record_lap("get-surf")

    if not xtgeo_surf:
        raise HTTPException(status_code=404, detail="Surface not found")

    surf_data_response = converters.to_api_surface_data(xtgeo_surf)
    perf_metrics.record_lap("convert")

    LOGGER.debug(f"Loaded realization surface in: {perf_metrics.to_string()}")

    return surf_data_response


@router.get("/statistical_surface_data/")
async def get_statistical_surface_data(
    response: Response,
    authenticated_user: AuthenticatedUser = Depends(AuthHelper.get_authenticated_user),
    case_uuid: str = Query(description="Sumo case uuid"),
    ensemble_name: str = Query(description="Ensemble name"),
    statistic_function: schemas.SurfaceStatisticFunction = Query(description="Statistics to calculate"),
    name: str = Query(description="Surface name"),
    attribute: str = Query(description="Surface attribute"),
    time_or_interval: Optional[str] = Query(None, description="Time point or time interval string"),
) -> schemas.SurfaceData:
    perf_metrics = PerfMetrics(response)

    access = await SurfaceAccess.from_case_uuid(authenticated_user.get_sumo_access_token(), case_uuid, ensemble_name)

    service_stat_func_to_compute = StatisticFunction.from_string_value(statistic_function)
    if service_stat_func_to_compute is None:
        raise HTTPException(status_code=404, detail="Invalid statistic requested")

    xtgeo_surf = access.get_statistical_surface_data(
        statistic_function=service_stat_func_to_compute,
        name=name,
        attribute=attribute,
        time_or_interval_str=time_or_interval,
    )
    perf_metrics.record_lap("sumo-calc")

    if not xtgeo_surf:
        raise HTTPException(status_code=404, detail="Could not find or compute surface")

    surf_data_response: schemas.SurfaceData = converters.to_api_surface_data(xtgeo_surf)
    perf_metrics.record_lap("convert")

    LOGGER.debug(f"Calculated statistical surface in: {perf_metrics.to_string()}")

    return surf_data_response


# pylint: disable=too-many-arguments
@router.get("/property_surface_resampled_to_static_surface/")
async def get_property_surface_resampled_to_static_surface(
    response: Response,
    authenticated_user: AuthenticatedUser = Depends(AuthHelper.get_authenticated_user),
    case_uuid: str = Query(description="Sumo case uuid"),
    ensemble_name: str = Query(description="Ensemble name"),
    realization_num_mesh: int = Query(description="Realization number"),
    name_mesh: str = Query(description="Surface name"),
    attribute_mesh: str = Query(description="Surface attribute"),
    realization_num_property: int = Query(description="Realization number"),
    name_property: str = Query(description="Surface name"),
    attribute_property: str = Query(description="Surface attribute"),
    time_or_interval_property: Optional[str] = Query(None, description="Time point or time interval string"),
) -> schemas.SurfaceData:
    perf_metrics = PerfMetrics(response)

    access = await SurfaceAccess.from_case_uuid(authenticated_user.get_sumo_access_token(), case_uuid, ensemble_name)
    xtgeo_surf_mesh = await access.get_realization_surface_data(
        real_num=realization_num_mesh, name=name_mesh, attribute=attribute_mesh
    )
    perf_metrics.record_lap("mesh-surf")

    xtgeo_surf_property = await access.get_realization_surface_data(
        real_num=realization_num_property,
        name=name_property,
        attribute=attribute_property,
        time_or_interval_str=time_or_interval_property,
    )
    perf_metrics.record_lap("prop-surf")

    if not xtgeo_surf_mesh or not xtgeo_surf_property:
        raise HTTPException(status_code=404, detail="Surface not found")

    resampled_surface = converters.resample_property_surface_to_mesh_surface(xtgeo_surf_mesh, xtgeo_surf_property)
    perf_metrics.record_lap("resample")

    surf_data_response: schemas.SurfaceData = converters.to_api_surface_data(resampled_surface)
    perf_metrics.record_lap("convert")

    LOGGER.debug(f"Loaded property surface in: {perf_metrics.to_string()}")

    return surf_data_response


@router.get("/property_surface_resampled_to_statistical_static_surface/")
async def get_property_surface_resampled_to_statistical_static_surface(
    authenticated_user: AuthenticatedUser = Depends(AuthHelper.get_authenticated_user),
    case_uuid: str = Query(description="Sumo case uuid"),
    ensemble_name: str = Query(description="Ensemble name"),
    statistic_function: schemas.SurfaceStatisticFunction = Query(description="Statistics to calculate"),
    name_mesh: str = Query(description="Surface name"),
    attribute_mesh: str = Query(description="Surface attribute"),
    # statistic_function_property: schemas.SurfaceStatisticFunction = Query(description="Statistics to calculate"),
    name_property: str = Query(description="Surface name"),
    attribute_property: str = Query(description="Surface attribute"),
    time_or_interval_property: Optional[str] = Query(None, description="Time point or time interval string"),
) -> schemas.SurfaceData:
    timer = PerfTimer()

    access = await SurfaceAccess.from_case_uuid(authenticated_user.get_sumo_access_token(), case_uuid, ensemble_name)
    service_stat_func_to_compute = StatisticFunction.from_string_value(statistic_function)
    if service_stat_func_to_compute is not None:
        xtgeo_surf_mesh = access.get_statistical_surface_data(
            statistic_function=service_stat_func_to_compute,
            name=name_mesh,
            attribute=attribute_mesh,
        )
        xtgeo_surf_property = access.get_statistical_surface_data(
            statistic_function=service_stat_func_to_compute,
            name=name_property,
            attribute=attribute_property,
            time_or_interval_str=time_or_interval_property,
        )

    if not xtgeo_surf_mesh or not xtgeo_surf_property:
        raise HTTPException(status_code=404, detail="Surface not found")

    resampled_surface = converters.resample_property_surface_to_mesh_surface(xtgeo_surf_mesh, xtgeo_surf_property)

    surf_data_response = converters.to_api_surface_data(resampled_surface)

    LOGGER.debug(f"Loaded property surface and created image, total time: {timer.elapsed_ms()}ms")

    return surf_data_response


@router.get("/large_download")
async def get_large_download(
    request: Request,
    response: Response,
    authenticated_user: AuthenticatedUser = Depends(AuthHelper.get_authenticated_user),
    real: int = Query(),
    dummy: int = Query(1)
) -> str:

    timer = PerfTimer()

    print(f"{real=}")
    print(f"{dummy=}")

    gridgeo = await get_grid_geometry(
        authenticated_user=authenticated_user,
        case_uuid="c619f32d-3ada-4e5e-8d3c-330f940e88f8",
        grid_name="Geogrid",
        ensemble_name="iter-0",
        realization=real)
    
    print(f"{type(gridgeo)=}")

    prop = await get_grid_parameter(
        authenticated_user=authenticated_user,
        case_uuid="c619f32d-3ada-4e5e-8d3c-330f940e88f8",
        grid_name="Geogrid",
        ensemble_name="iter-0",
        realization=real,
        parameter_name="FACIES")
    
    print(f"{type(prop)=}")

    print(f"Download took: {timer.elapsed_s():2f}s")

    return "large_download"




@router.post("/surface_intersections/")
async def get_surface_intersections(
    request: Request,
    response: Response,
    authenticated_user: AuthenticatedUser = Depends(AuthHelper.get_authenticated_user),
    case_uuid: str = Query(description="Sumo case uuid"),
    ensemble_name: str = Query(description="Ensemble name"),
    name: str = Query(description="Surface names"),
    attribute: str = Query(description="Surface attribute"),
    num_reals: int = Query(None, description="Number of realizations to intersect"),
    num_workers: int = Query(None, description="Number of workers to use"),
    cutting_plane: schemas.CuttingPlane = Body(embed=True),
) -> List[schemas.SurfaceIntersectionData]:

    perf_metrics = PerfMetrics(response)

    # intersections = await execute_usersession_job_calc_surf_intersections_fetch_first(
    #     request,
    #     authenticated_user,
    #     case_uuid,
    #     ensemble_name,
    #     name,
    #     attribute,
    #     num_reals,
    #     cutting_plane,
    # )

    # return intersections


    # intersections = await execute_usersession_job_calc_surf_intersections_queue(
    #     request,
    #     authenticated_user,
    #     case_uuid,
    #     ensemble_name,
    #     name,
    #     attribute,
    #     num_reals,
    #     num_workers,
    #     cutting_plane,
    # )

    # return intersections

    intersections = await _calc_surf_intersections_aiomulti(
        authenticated_user,
        case_uuid,
        ensemble_name,
        name,
        attribute,
        num_reals,
        num_workers,
        cutting_plane,
    )

    LOGGER.debug(f"Intersected {len(intersections)} surfaces in: {perf_metrics.to_string()}")

    return intersections


# --------------------------------------------------------------------------------------

@dataclass
class SurfItem:
    access_token: str
    case_uuid: str
    ensemble_name: str
    name: str
    attribute: str
    real: int
    #fence_arr: np.ndarray

@dataclass
class ResultItem:
    perf_info:  str
    line: np.ndarray


global_access = None
global_fence_arr = None


def init_access_and_fence(access_token: str, case_uuid: str, ensemble_name: str, fence_arr: np.ndarray):
    global global_access
    global global_fence_arr
    global_access = asyncio.run(SurfaceAccess.from_case_uuid(access_token, case_uuid, ensemble_name))
    global_fence_arr = fence_arr


async def process_a_surf(item: SurfItem) -> ResultItem:
    print(f"fetch_a_surf {item.real=}")
    perf_metrics = PerfMetrics()

    access = global_access

    xtgeo_surf = await access.get_realization_surface_data(real_num=item.real, name=item.name, attribute=item.attribute)
    if xtgeo_surf is None:
        return None
    perf_metrics.record_lap("fetch")

    line = xtgeo_surf.get_randomline(global_fence_arr)
    perf_metrics.record_lap("calc")

    res_item = ResultItem(perf_info=perf_metrics.to_string(), line=line)
    return res_item


async def _calc_surf_intersections_aiomulti(
    authenticated_user: AuthenticatedUser,
    case_uuid: str,
    ensemble_name: str,
    name: str,
    attribute: str,
    num_reals: int,
    num_workers: int,
    cutting_plane: schemas.CuttingPlane,
) -> List[schemas.SurfaceIntersectionData]:

    myprefix = ">>>>>>>>>>>>>>>>> _calc_surf_intersections_aiomulti():"
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
        ))

    print(f"{myprefix} built item_list {len(item_list)=}")

    # See
    # https://aiomultiprocess.omnilib.dev/en/latest/guide.html

    processes = 4
    queuecount = None
    childconcurrency = 4

    async with Pool(queuecount=queuecount, processes=processes, childconcurrency=childconcurrency, initializer=init_access_and_fence, initargs=[access_token, case_uuid, ensemble_name, fence_arr]) as pool:
        print(f"{myprefix} pool info {pool.process_count=}")
        print(f"{myprefix} pool info {pool.queue_count=}")
        print(f"{myprefix} pool info {pool.childconcurrency=}")

        intersections = []
        async for res_item in pool.map(process_a_surf, item_list):
            if res_item is not None:
                isecdata = schemas.SurfaceIntersectionData(name="someName", hlen_arr=res_item.line[:, 0].tolist(), z_arr=res_item.line[:, 1].tolist())
                intersections.append(isecdata)
                print(f"{myprefix} got isec {len(intersections)}  perf_info={res_item.perf_info}")

    print(f"{myprefix} finished")

    return intersections


# --------------------------------------------------------------------------------------
async def execute_usersession_job_calc_surf_intersections_queue(
    fastApiRequest: Request,
    authenticated_user: AuthenticatedUser,
    case_uuid: str,
    ensemble_name: str,
    name: str,
    attribute: str,
    num_reals: int,
    num_workers: int,
    cutting_plane: schemas.CuttingPlane,
) -> List[schemas.SurfaceIntersectionData]:

    print(">>>>>>>>>>>>>>>>> execute_usersession_job_calc_surf_intersections_queue() started")

    query_params = {
        "case_uuid": case_uuid,
        "ensemble_name": ensemble_name,
        "name": name,
        "attribute": attribute,
        "num_reals": num_reals,
        "num_workers": num_workers,
     }

    base_url = await get_user_session_base_url(authenticated_user)
    url = httpx.URL(path="/surface/calc_surf_intersections_queue")
    client = httpx.AsyncClient(base_url=base_url)
    job_req = client.build_request(
        method="POST",
        url=url,
        params=query_params,
        #json={"cutting_plane": cutting_plane.model_dump()},
        data=json.dumps({"cutting_plane": cutting_plane.model_dump()}),
        cookies=fastApiRequest.cookies,
        timeout=600,
    )

    job_resp = await client.send(job_req)

    print(">>>>>>>>>>>>>>>>> execute_usersession_job_calc_surf_intersections_queue() finished")

    return job_resp.json()


# --------------------------------------------------------------------------------------
async def execute_usersession_job_sigtest(
    fastApiRequest: Request,
    authenticated_user: AuthenticatedUser,
    case_uuid: str,
    ensemble_name: str,
    name: str,
    attribute: str,
    num_reals: int,
    num_workers: int,
    cutting_plane: schemas.CuttingPlane,
) -> None:

    print(">>>>>>>>>>>>>>>>> execute_usersession_job_sigtest() started")

    query_params = {
        "case_uuid": case_uuid,
        "ensemble_name": ensemble_name,
        "name": name,
        "attribute": attribute,
        "num_reals": num_reals,
        "num_workers": num_workers,
    }

    # print("------------------------------")
    # print(f"{cutting_plane.model_dump()=}")
    # print("------------------------------")

    base_url = await get_user_session_base_url(authenticated_user)
    url = httpx.URL(path="/surface/sigtest")
    client = httpx.AsyncClient(base_url=base_url)
    job_req = client.build_request(
        method="POST",
        url=url,
        params=query_params,
        #json={"cutting_plane": cutting_plane.model_dump()},
        data=json.dumps({"cutting_plane": cutting_plane.model_dump()}),
        cookies=fastApiRequest.cookies,
        timeout=600,
    )

    print("------------------------------")
    print(f"{url=}")
    print(f"{job_req=}")
    #print(f"{job_req.content=}")
    print("------------------------------")

    job_resp = await client.send(job_req)

    print(f"{job_resp.text=} {job_resp.status_code=}")

    print(">>>>>>>>>>>>>>>>> execute_usersession_job_sigtest() finished")
