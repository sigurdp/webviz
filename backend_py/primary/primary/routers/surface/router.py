import asyncio
import logging
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Response, Body
from webviz_pkg.core_utils.perf_timer import PerfTimer
from webviz_pkg.core_utils.perf_metrics import PerfMetrics

from primary.services.sumo_access.case_inspector import CaseInspector
from primary.services.sumo_access.surface_access import SurfaceAccess
from primary.services.smda_access.stratigraphy_access import StratigraphyAccess, StratigraphicUnit
from primary.services.smda_access.stratigraphy_utils import sort_stratigraphic_names_by_hierarchy
from primary.services.smda_access.mocked_drogon_smda_access import _mocked_stratigraphy_access
from primary.services.utils.statistic_function import StatisticFunction
from primary.services.utils.surface_intersect_with_polyline import intersect_surface_with_polyline
from primary.services.utils.authenticated_user import AuthenticatedUser
from primary.auth.auth_helper import AuthHelper
from primary.utils.response_perf_metrics import ResponsePerfMetrics
from primary.services.surface_query_service.surface_query_service import batch_sample_surface_in_points_async
from primary.services.surface_query_service.surface_query_service import RealizationSampleResult

from . import converters
from . import schemas


LOGGER = logging.getLogger(__name__)

router = APIRouter()


@router.get("/realization_surfaces_metadata/")
async def get_realization_surfaces_metadata(
    response: Response,
    authenticated_user: AuthenticatedUser = Depends(AuthHelper.get_authenticated_user),
    case_uuid: str = Query(description="Sumo case uuid"),
    ensemble_name: str = Query(description="Ensemble name"),
) -> schemas.SurfaceMetaSet:
    """
    Get metadata for realization surfaces in a Sumo ensemble
    """
    perf_metrics = ResponsePerfMetrics(response)

    async with asyncio.TaskGroup() as tg:
        access = SurfaceAccess.from_case_uuid(authenticated_user.get_sumo_access_token(), case_uuid, ensemble_name)
        surf_meta_task = tg.create_task(access.get_realization_surfaces_metadata_async())
        surf_meta_task.add_done_callback(lambda _: perf_metrics.record_lap_no_reset("get-meta"))

        strat_units_task = tg.create_task(_get_stratigraphic_units_for_case_async(authenticated_user, case_uuid))
        strat_units_task.add_done_callback(lambda _: perf_metrics.record_lap_no_reset("get-strat"))

    perf_metrics.reset_lap_timer()
    sumo_surf_meta_set = surf_meta_task.result()
    strat_units = strat_units_task.result()

    sorted_stratigraphic_surfaces = sort_stratigraphic_names_by_hierarchy(strat_units)
    api_surf_meta_set = converters.to_api_surface_meta_set(sumo_surf_meta_set, sorted_stratigraphic_surfaces)
    perf_metrics.record_lap("compose")

    LOGGER.info(f"Got metadata for realization surfaces in: {perf_metrics.to_string()}")

    return api_surf_meta_set


@router.get("/observed_surfaces_metadata/")
async def get_observed_surfaces_metadata(
    response: Response,
    authenticated_user: AuthenticatedUser = Depends(AuthHelper.get_authenticated_user),
    case_uuid: str = Query(description="Sumo case uuid"),
) -> schemas.SurfaceMetaSet:
    """
    Get metadata for observed surfaces in a Sumo case
    """
    perf_metrics = ResponsePerfMetrics(response)

    async with asyncio.TaskGroup() as tg:
        access = SurfaceAccess.from_case_uuid_no_iteration(authenticated_user.get_sumo_access_token(), case_uuid)
        surf_meta_task = tg.create_task(access.get_observed_surfaces_metadata_async())
        surf_meta_task.add_done_callback(lambda _: perf_metrics.record_lap_no_reset("get-meta"))

        strat_units_task = tg.create_task(_get_stratigraphic_units_for_case_async(authenticated_user, case_uuid))
        strat_units_task.add_done_callback(lambda _: perf_metrics.record_lap_no_reset("get-strat"))

    perf_metrics.reset_lap_timer()
    sumo_surf_meta_set = surf_meta_task.result()
    strat_units = strat_units_task.result()

    sorted_stratigraphic_surfaces = sort_stratigraphic_names_by_hierarchy(strat_units)
    api_surf_meta_set = converters.to_api_surface_meta_set(sumo_surf_meta_set, sorted_stratigraphic_surfaces)
    perf_metrics.record_lap("compose")

    LOGGER.info(f"Got metadata for observed surfaces in: {perf_metrics.to_string()}")

    return api_surf_meta_set


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
    perf_metrics = ResponsePerfMetrics(response)

    access = SurfaceAccess.from_case_uuid(authenticated_user.get_sumo_access_token(), case_uuid, ensemble_name)
    xtgeo_surf = await access.get_realization_surface_data_async(
        real_num=realization_num, name=name, attribute=attribute, time_or_interval_str=time_or_interval
    )
    perf_metrics.record_lap("get-surf")

    if not xtgeo_surf:
        raise HTTPException(status_code=404, detail="Surface not found")

    surf_data_response = converters.to_api_surface_data(xtgeo_surf)
    perf_metrics.record_lap("convert")

    LOGGER.info(f"Loaded realization surface in: {perf_metrics.to_string()}")

    return surf_data_response


@router.get("/observed_surface_data/")
async def get_observed_surface_data(
    response: Response,
    authenticated_user: AuthenticatedUser = Depends(AuthHelper.get_authenticated_user),
    case_uuid: str = Query(description="Sumo case uuid"),
    name: str = Query(description="Surface name"),
    attribute: str = Query(description="Surface attribute"),
    time_or_interval: str = Query(description="Time point or time interval string"),
) -> schemas.SurfaceData:
    perf_metrics = ResponsePerfMetrics(response)

    access = SurfaceAccess.from_case_uuid_no_iteration(authenticated_user.get_sumo_access_token(), case_uuid)
    xtgeo_surf = await access.get_observed_surface_data_async(
        name=name, attribute=attribute, time_or_interval_str=time_or_interval
    )
    perf_metrics.record_lap("get-surf")

    if not xtgeo_surf:
        raise HTTPException(status_code=404, detail="Surface not found")

    surf_data_response = converters.to_api_surface_data(xtgeo_surf)
    perf_metrics.record_lap("convert")

    LOGGER.info(f"Loaded observed surface in: {perf_metrics.to_string()}")

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
    perf_metrics = ResponsePerfMetrics(response)

    access = SurfaceAccess.from_case_uuid(authenticated_user.get_sumo_access_token(), case_uuid, ensemble_name)

    service_stat_func_to_compute = StatisticFunction.from_string_value(statistic_function)
    if service_stat_func_to_compute is None:
        raise HTTPException(status_code=404, detail="Invalid statistic requested")

    xtgeo_surf = await access.get_statistical_surface_data_async(
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

    LOGGER.info(f"Calculated statistical surface in: {perf_metrics.to_string()}")

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
    perf_metrics = ResponsePerfMetrics(response)

    access = SurfaceAccess.from_case_uuid(authenticated_user.get_sumo_access_token(), case_uuid, ensemble_name)
    xtgeo_surf_mesh = await access.get_realization_surface_data_async(
        real_num=realization_num_mesh, name=name_mesh, attribute=attribute_mesh
    )
    perf_metrics.record_lap("mesh-surf")

    xtgeo_surf_property = await access.get_realization_surface_data_async(
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

    LOGGER.info(f"Loaded property surface in: {perf_metrics.to_string()}")

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

    access = SurfaceAccess.from_case_uuid(authenticated_user.get_sumo_access_token(), case_uuid, ensemble_name)
    service_stat_func_to_compute = StatisticFunction.from_string_value(statistic_function)
    if service_stat_func_to_compute is not None:
        xtgeo_surf_mesh = await access.get_statistical_surface_data_async(
            statistic_function=service_stat_func_to_compute,
            name=name_mesh,
            attribute=attribute_mesh,
        )
        xtgeo_surf_property = await access.get_statistical_surface_data_async(
            statistic_function=service_stat_func_to_compute,
            name=name_property,
            attribute=attribute_property,
            time_or_interval_str=time_or_interval_property,
        )

    if not xtgeo_surf_mesh or not xtgeo_surf_property:
        raise HTTPException(status_code=404, detail="Surface not found")

    resampled_surface = converters.resample_property_surface_to_mesh_surface(xtgeo_surf_mesh, xtgeo_surf_property)

    surf_data_response = converters.to_api_surface_data(resampled_surface)

    LOGGER.info(f"Loaded property surface and created image, total time: {timer.elapsed_ms()}ms")

    return surf_data_response


@router.post("/get_surface_intersection")
async def post_get_surface_intersection(
    authenticated_user: AuthenticatedUser = Depends(AuthHelper.get_authenticated_user),
    case_uuid: str = Query(description="Sumo case uuid"),
    ensemble_name: str = Query(description="Ensemble name"),
    realization_num: int = Query(description="Realization number"),
    name: str = Query(description="Surface name"),
    attribute: str = Query(description="Surface attribute"),
    time_or_interval_str: Optional[str] = Query(None, description="Time point or time interval string"),
    cumulative_length_polyline: schemas.SurfaceIntersectionCumulativeLengthPolyline = Body(embed=True),
) -> schemas.SurfaceIntersectionData:
    """Get surface intersection data for requested surface name.

    The surface intersection data for surface name contains: An array of z-points, i.e. one z-value/depth per (x, y)-point in polyline,
    and cumulative lengths, the accumulated length at each z-point in the array.
    """
    access = SurfaceAccess.from_case_uuid(authenticated_user.get_sumo_access_token(), case_uuid, ensemble_name)

    surface = await access.get_realization_surface_data_async(
        real_num=realization_num, name=name, attribute=attribute, time_or_interval_str=time_or_interval_str
    )
    if surface is None:
        raise HTTPException(status_code=404, detail="Surface '{name}' not found")

    # Ensure name is applied
    surface.name = name

    intersection_polyline = converters.from_api_cumulative_length_polyline_to_xtgeo_polyline(cumulative_length_polyline)
    surface_intersection = intersect_surface_with_polyline(surface, intersection_polyline)

    surface_intersection_response = converters.to_api_surface_intersection(surface_intersection)

    return surface_intersection_response


@router.post("/sample_surface_in_points")
async def post_sample_surface_in_points(
    case_uuid: str = Query(description="Sumo case uuid"),
    ensemble_name: str = Query(description="Ensemble name"),
    surface_name: str = Query(description="Surface name"),
    surface_attribute: str = Query(description="Surface attribute"),
    realization_nums: List[int] = Query(description="Realization numbers"),
    sample_points: schemas.PointSetXY = Body(embed=True),
    authenticated_user: AuthenticatedUser = Depends(AuthHelper.get_authenticated_user),
) -> List[schemas.SurfaceRealizationSampleValues]:

    sumo_access_token = authenticated_user.get_sumo_access_token()

    result_arr: List[RealizationSampleResult] = await batch_sample_surface_in_points_async(
        sumo_access_token=sumo_access_token,
        case_uuid=case_uuid,
        iteration_name=ensemble_name,
        surface_name=surface_name,
        surface_attribute=surface_attribute,
        realizations=realization_nums,
        x_coords=sample_points.x_points,
        y_coords=sample_points.y_points,
    )

    intersections: List[schemas.SurfaceRealizationSampleValues] = []
    for res in result_arr:
        intersections.append(
            schemas.SurfaceRealizationSampleValues(
                realization=res.realization,
                sampled_values=res.sampledValues,
            )
        )

    return intersections


async def _get_stratigraphic_units_for_case_async(
    authenticated_user: AuthenticatedUser, case_uuid: str
) -> list[StratigraphicUnit]:
    perf_metrics = PerfMetrics()

    case_inspector = CaseInspector.from_case_uuid(authenticated_user.get_sumo_access_token(), case_uuid)
    strat_column_identifier = await case_inspector.get_stratigraphic_column_identifier_async()
    perf_metrics.record_lap("get-strat-ident")

    strat_access: StratigraphyAccess | _mocked_stratigraphy_access.StratigraphyAccess
    if strat_column_identifier == "DROGON_HAS_NO_STRATCOLUMN":
        strat_access = _mocked_stratigraphy_access.StratigraphyAccess(authenticated_user.get_smda_access_token())
    else:
        strat_access = StratigraphyAccess(authenticated_user.get_smda_access_token())

    strat_units = await strat_access.get_stratigraphic_units(strat_column_identifier)
    perf_metrics.record_lap("get-strat-units")

    LOGGER.info(f"Got stratigraphic units for case in : {perf_metrics.to_string()}")

    return strat_units
