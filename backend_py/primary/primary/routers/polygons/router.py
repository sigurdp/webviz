import logging
from typing import List, Union

from fastapi import APIRouter, Depends, HTTPException, Query
from webviz_pkg.core_utils.perf_timer import PerfTimer

from primary.auth.auth_helper import AuthHelper
from primary.services.smda_access.drogon import DrogonSmdaAccess
from primary.services.smda_access import SmdaAccess
from primary.services.smda_access.stratigraphy_utils import sort_stratigraphic_names_by_hierarchy
from primary.services.sumo_access.case_inspector import CaseInspector
from primary.services.sumo_access.polygons_access import PolygonsAccess
from primary.services.utils.authenticated_user import AuthenticatedUser
from primary.utils.drogon import is_drogon_identifier

from . import converters, schemas

LOGGER = logging.getLogger(__name__)

router = APIRouter()


@router.get("/polygons_directory/")
async def get_polygons_directory(
    authenticated_user: AuthenticatedUser = Depends(AuthHelper.get_authenticated_user),
    case_uuid: str = Query(description="Sumo case uuid"),
    ensemble_name: str = Query(description="Ensemble name"),
) -> List[schemas.PolygonsMeta]:
    """
    Get a directory of polygons in a Sumo ensemble
    """
    access = PolygonsAccess.from_iteration_name(authenticated_user.get_sumo_access_token(), case_uuid, ensemble_name)
    polygons_dir = await access.get_polygons_directory_async()

    case_inspector = CaseInspector.from_case_uuid(authenticated_user.get_sumo_access_token(), case_uuid)
    strat_column_identifier = await case_inspector.get_stratigraphic_column_identifier_async()
    smda_access: Union[SmdaAccess, DrogonSmdaAccess]

    if is_drogon_identifier(strat_column_identifier=strat_column_identifier):
        smda_access = DrogonSmdaAccess()
    else:
        smda_access = SmdaAccess(authenticated_user.get_smda_access_token())
    strat_units = await smda_access.get_stratigraphic_units_async(strat_column_identifier)
    sorted_stratigraphic_surfaces = sort_stratigraphic_names_by_hierarchy(strat_units)

    return converters.to_api_polygons_directory(polygons_dir, sorted_stratigraphic_surfaces)


@router.get("/polygons_data/")
async def get_polygons_data(
    authenticated_user: AuthenticatedUser = Depends(AuthHelper.get_authenticated_user),
    case_uuid: str = Query(description="Sumo case uuid"),
    ensemble_name: str = Query(description="Ensemble name"),
    realization_num: int = Query(description="Realization number"),
    name: str = Query(description="Surface name"),
    attribute: str = Query(description="Surface attribute"),
) -> List[schemas.PolygonData]:
    timer = PerfTimer()

    access = PolygonsAccess.from_iteration_name(authenticated_user.get_sumo_access_token(), case_uuid, ensemble_name)
    xtgeo_poly = await access.get_polygons_async(real_num=realization_num, name=name, attribute=attribute)

    if not xtgeo_poly:
        raise HTTPException(status_code=404, detail="Polygons not found")

    poly_data_response = converters.to_api_polygons_data(xtgeo_poly)

    LOGGER.debug(f"Loaded polygons and created response, total time: {timer.elapsed_ms()}ms")
    return poly_data_response
