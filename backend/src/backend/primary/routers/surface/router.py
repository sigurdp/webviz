import logging
from typing import List, Union, Optional
import io
import xtgeo
import numpy as np

from fastapi import APIRouter, Depends, HTTPException, Query, Response

from src.services.sumo_access.surface_access import SurfaceAccess
from src.services.sumo_access.case_inspector import CaseInspector
from src.services.smda_access.stratigraphy_access import StratigraphyAccess
from src.services.smda_access.stratigraphy_utils import sort_stratigraphic_names_by_hierarchy
from src.services.smda_access.mocked_drogon_smda_access import _mocked_stratigraphy_access
from src.services.utils.statistic_function import StatisticFunction
from src.services.utils.authenticated_user import AuthenticatedUser
from src.services.utils.perf_timer import PerfTimer
from src.backend.auth.auth_helper import AuthHelper


from . import converters
from . import schemas

from src.backend.caching import get_user_cache
from src.backend.utils.perf_metrics import PerfMetrics
from fastapi import BackgroundTasks


LOGGER = logging.getLogger(__name__)

router = APIRouter()


@router.get("/surface_directory/")
def get_surface_directory(
    authenticated_user: AuthenticatedUser = Depends(AuthHelper.get_authenticated_user),
    case_uuid: str = Query(description="Sumo case uuid"),
    ensemble_name: str = Query(description="Ensemble name"),
) -> List[schemas.SurfaceMeta]:
    """
    Get a directory of surfaces in a Sumo ensemble
    """
    surface_access = SurfaceAccess(authenticated_user.get_sumo_access_token(), case_uuid, ensemble_name)
    sumo_surf_dir = surface_access.get_surface_directory()

    case_inspector = CaseInspector(authenticated_user.get_sumo_access_token(), case_uuid)
    strat_column_identifier = case_inspector.get_stratigraphic_column_identifier()
    strat_access: Union[StratigraphyAccess, _mocked_stratigraphy_access.StratigraphyAccess]

    if strat_column_identifier == "DROGON_HAS_NO_STRATCOLUMN":
        strat_access = _mocked_stratigraphy_access.StratigraphyAccess(authenticated_user.get_smda_access_token())
    else:
        strat_access = StratigraphyAccess(authenticated_user.get_smda_access_token())
    strat_units = strat_access.get_stratigraphic_units(strat_column_identifier)
    sorted_stratigraphic_surfaces = sort_stratigraphic_names_by_hierarchy(strat_units)

    return converters.to_api_surface_directory(sumo_surf_dir, sorted_stratigraphic_surfaces)


@router.get("/realization_surface_data/")
async def get_realization_surface_data(
    response: Response,
    background_tasks: BackgroundTasks,
    authenticated_user: AuthenticatedUser = Depends(AuthHelper.get_authenticated_user),
    case_uuid: str = Query(description="Sumo case uuid"),
    ensemble_name: str = Query(description="Ensemble name"),
    realization_num: int = Query(description="Realization number"),
    name: str = Query(description="Surface name"),
    attribute: str = Query(description="Surface attribute"),
    time_or_interval: Optional[str] = Query(None, description="Time point or time interval string"),
) -> schemas.SurfaceData:
    perf_metrics = PerfMetrics(response)

    cache = get_user_cache(authenticated_user)
    cache_key = f"surface:{case_uuid}_{ensemble_name}_{realization_num}_{name}_{attribute}_{time_or_interval}"

    # cache_key = f"surf_data_response_{case_uuid}_{ensemble_name}_{realization_num}_{name}_{attribute}_{time_or_interval}"
    # cached_surf_data_response = await CACHE.get_Any(cache_key)
    # if cached_surf_data_response:
    #     LOGGER.debug(f"Loaded surface from cache, total time: {timer.elapsed_ms()}ms")
    #     return cached_surf_data_response

    cached_xtgeo_surf = await cache.get_RegularSurface(cache_key)
    xtgeo_surf = cached_xtgeo_surf
    perf_metrics.register_lap("cache")

    if not xtgeo_surf:
        access = SurfaceAccess(authenticated_user.get_sumo_access_token(), case_uuid, ensemble_name)
        xtgeo_surf = access.get_realization_surface_data(
            real_num=realization_num, name=name, attribute=attribute, time_or_interval_str=time_or_interval
        )
        perf_metrics.register_lap("sumo-get")

    if not xtgeo_surf:
        raise HTTPException(status_code=404, detail="Surface not found")

    surf_data_response = converters.to_api_surface_data(xtgeo_surf)
    perf_metrics.register_lap("convert")
    # await CACHE.set_Any(cache_key, surf_data_response)

    if not cached_xtgeo_surf:
        background_tasks.add_task(cache.set_RegularSurface, cache_key, xtgeo_surf)
        # await CACHE.set_RegularSurface(cache_key, xtgeo_surf)

    LOGGER.debug(f"Loaded surface, total time: {perf_metrics.get_elapsed_ms()}ms ({perf_metrics.get_as_string()})")

    return surf_data_response


@router.get("/statistical_surface_data/")
def get_statistical_surface_data(
    authenticated_user: AuthenticatedUser = Depends(AuthHelper.get_authenticated_user),
    case_uuid: str = Query(description="Sumo case uuid"),
    ensemble_name: str = Query(description="Ensemble name"),
    statistic_function: schemas.SurfaceStatisticFunction = Query(description="Statistics to calculate"),
    name: str = Query(description="Surface name"),
    attribute: str = Query(description="Surface attribute"),
    time_or_interval: Optional[str] = Query(None, description="Time point or time interval string"),
) -> schemas.SurfaceData:
    timer = PerfTimer()

    access = SurfaceAccess(authenticated_user.get_sumo_access_token(), case_uuid, ensemble_name)

    service_stat_func_to_compute = StatisticFunction.from_string_value(statistic_function)
    if service_stat_func_to_compute is not None:
        xtgeo_surf = access.get_statistical_surface_data(
            statistic_function=service_stat_func_to_compute,
            name=name,
            attribute=attribute,
            time_or_interval_str=time_or_interval,
        )

    if not xtgeo_surf:
        raise HTTPException(status_code=404, detail="Could not find or compute surface")

    surf_data_response = converters.to_api_surface_data(xtgeo_surf)

    LOGGER.debug(f"Calculated statistical dynamic surface and created image, total time: {timer.elapsed_ms()}ms")

    return surf_data_response


# pylint: disable=too-many-arguments
@router.get("/property_surface_resampled_to_static_surface/")
def get_property_surface_resampled_to_static_surface(
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

    access = SurfaceAccess(authenticated_user.get_sumo_access_token(), case_uuid, ensemble_name)
    xtgeo_surf_mesh = access.get_realization_surface_data(
        real_num=realization_num_mesh, name=name_mesh, attribute=attribute_mesh
    )
    perf_metrics.register_lap("mesh-surf")

    xtgeo_surf_property = access.get_realization_surface_data(
        real_num=realization_num_property,
        name=name_property,
        attribute=attribute_property,
        time_or_interval_str=time_or_interval_property,
    )
    perf_metrics.register_lap("prop-surf")

    if not xtgeo_surf_mesh or not xtgeo_surf_property:
        raise HTTPException(status_code=404, detail="Surface not found")

    resampled_surface = converters.resample_property_surface_to_mesh_surface(xtgeo_surf_mesh, xtgeo_surf_property)
    perf_metrics.register_lap("resample")

    surf_data_response = converters.to_api_surface_data(resampled_surface)
    perf_metrics.register_lap("conv")

    LOGGER.debug(f"Loaded property surface, total time: {perf_metrics.get_elapsed_ms()}ms ({perf_metrics.get_as_string()})")

    return surf_data_response


@router.get("/property_surface_resampled_to_statistical_static_surface/")
def get_property_surface_resampled_to_statistical_static_surface(
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

    access = SurfaceAccess(authenticated_user.get_sumo_access_token(), case_uuid, ensemble_name)
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
