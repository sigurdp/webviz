import asyncio
import numpy as np
import logging
from typing import List, Union, Optional

from src.services.sumo_access.surface_access import SurfaceAccess
from src.services.utils.authenticated_user import AuthenticatedUser
from src.backend.primary.routers.surface import schemas
from src.backend.utils.perf_metrics import PerfMetrics

LOGGER = logging.getLogger(__name__)


async def calc_surf_isec_fetch_first(
    perf_metrics: PerfMetrics,
    authenticated_user: AuthenticatedUser,
    case_uuid: str,
    ensemble_name: str,
    name: str,
    attribute: str,
    num_reals: int,
    cutting_plane: schemas.CuttingPlane,
) -> List[schemas.SurfaceIntersectionData]:
    myprefix = ">>>>>>>>>>>>>>>>> calc_surf_isec_fetch_first():"
    print(f"{myprefix} started")

    perf_metrics.reset_lap_timer()

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

    print(f"{myprefix} finished")

    return intersections
