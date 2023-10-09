import logging
from io import BytesIO
from typing import List, Optional

import xtgeo
from fmu.sumo.explorer import TimeFilter, TimeType
from fmu.sumo.explorer.objects import SurfaceCollection, Surface

from src.services.utils.perf_timer import PerfTimer
from src.services.utils.statistic_function import StatisticFunction

from ._helpers import SumoEnsemble
from .surface_types import SurfaceMeta
from .generic_types import SumoContent

LOGGER = logging.getLogger(__name__)


class SurfaceAccess(SumoEnsemble):
    async def get_surface_directory(self) -> List[SurfaceMeta]:
        surface_collection: SurfaceCollection = self._case.surfaces.filter(
            iteration=self._iteration_name,
            aggregation=False,
            realization=0,
        )

        surfs: List[SurfaceMeta] = []
        async for surf in surface_collection:
            iso_string_or_time_interval = None

            t_start = surf["data"].get("time", {}).get("t0", {}).get("value", None)
            t_end = surf["data"].get("time", {}).get("t1", {}).get("value", None)
            if t_start and not t_end:
                iso_string_or_time_interval = t_start
            if t_start and t_end:
                iso_string_or_time_interval = f"{t_start}/{t_end}"

            surf_meta = SurfaceMeta(
                name=surf["data"]["name"],
                tagname=surf["data"].get("tagname", "Unknown"),
                iso_date_or_interval=iso_string_or_time_interval,
                content=surf["data"].get("content", SumoContent.DEPTH),
                is_observation=surf["data"]["is_observation"],
                is_stratigraphic=surf["data"]["stratigraphic"],
                zmin=surf["data"]["bbox"]["zmin"],
                zmax=surf["data"]["bbox"]["zmax"],
            )

            surfs.append(surf_meta)

        return surfs

    async def get_realization_surface_data(
        self, real_num: int, name: str, attribute: str, time_or_interval_str: Optional[str] = None
    ) -> Optional[xtgeo.RegularSurface]:
        """
        Get surface data for a realization surface
        """
        timer = PerfTimer()
        addr_str = self._make_addr_str(real_num, name, attribute, time_or_interval_str)

        if time_or_interval_str is None:
            time_filter = TimeFilter(TimeType.NONE)

        else:
            timestamp_arr = time_or_interval_str.split("/", 1)
            if len(timestamp_arr) == 0 or len(timestamp_arr) > 2:
                raise ValueError("time_or_interval_str must contain a single timestamp or interval")
            if len(timestamp_arr) == 1:
                time_filter = TimeFilter(
                    TimeType.TIMESTAMP,
                    start=timestamp_arr[0],
                    end=timestamp_arr[0],
                    exact=True,
                )
            else:
                time_filter = TimeFilter(
                    TimeType.INTERVAL,
                    start=timestamp_arr[0],
                    end=timestamp_arr[1],
                    exact=True,
                )

        surface_collection: SurfaceCollection = self._case.surfaces.filter(
            iteration=self._iteration_name,
            aggregation=False,
            realization=real_num,
            name=name,
            tagname=attribute,
            time=time_filter,
        )

        surf_count = await surface_collection.length_async()
        if surf_count == 0:
            LOGGER.warning(f"No realization surface found in Sumo for {addr_str}")
            return None
        if surf_count > 1:
            LOGGER.warning(f"Multiple ({surf_count}) surfaces found in Sumo for: {addr_str}. Returning first surface.")

        sumo_surf: Surface = await surface_collection.getitem_async(0)
        et_locate_ms = timer.lap_ms()

        byte_stream: BytesIO = await sumo_surf.blob_async
        et_download_ms = timer.lap_ms()

        xtgeo_surf = xtgeo.surface_from_file(byte_stream)
        et_xtgeo_read_ms = timer.lap_ms()

        size_mb = byte_stream.getbuffer().nbytes/(1024*1024)
        nx = xtgeo_surf.ncol
        ny = xtgeo_surf.nrow

        LOGGER.debug(
            f"Got realization surface from Sumo in: {timer.elapsed_ms()}ms ("
            f"locate={et_locate_ms}ms, "
            f"download={et_download_ms}ms, "
            f"xtgeo_read={et_xtgeo_read_ms}ms) "
            f"[{nx}x{ny}, {size_mb:.2f}MB] "
            f"({addr_str})"
        )

        return xtgeo_surf

    def get_statistical_surface_data(
        self,
        statistic_function: StatisticFunction,
        name: str,
        attribute: str,
        time_or_interval_str: Optional[str] = None,
    ) -> Optional[xtgeo.RegularSurface]:
        """
        Compute statistic and return surface data
        """
        timer = PerfTimer()
        addr_str = self._make_addr_str(-1, name, attribute, time_or_interval_str)

        if time_or_interval_str is None:
            time_filter = TimeFilter(TimeType.NONE)

        else:
            timestamp_arr = time_or_interval_str.split("/", 1)
            if len(timestamp_arr) == 0 or len(timestamp_arr) > 2:
                raise ValueError("time_or_interval_str must contain a single timestamp or interval")
            if len(timestamp_arr) == 1:
                time_filter = TimeFilter(
                    TimeType.TIMESTAMP,
                    start=timestamp_arr[0],
                    end=timestamp_arr[0],
                    exact=True,
                )
            else:
                time_filter = TimeFilter(
                    TimeType.INTERVAL,
                    start=timestamp_arr[0],
                    end=timestamp_arr[1],
                    exact=True,
                )
        et_get_case_ms = timer.lap_ms()

        surface_collection = self._case.surfaces.filter(
            iteration=self._iteration_name,
            aggregation=False,
            name=name,
            tagname=attribute,
            time=time_filter,
        )
        et_collect_surfaces_ms = timer.lap_ms()

        surf_count = len(surface_collection)
        if surf_count == 0:
            LOGGER.warning(f"No statistical surfaces found in Sumo for {addr_str}")
            return None

        realizations = surface_collection.realizations

        xtgeo_surf = _compute_statistical_surface(statistic_function, surface_collection)
        et_calc_stat_ms = timer.lap_ms()

        if not xtgeo_surf:
            LOGGER.warning(f"Could not calculate statistical surface using Sumo for {addr_str}")
            return None

        LOGGER.debug(
            f"Calculated statistical surface using Sumo in: {timer.elapsed_ms()}ms ("
            f"get_case={et_get_case_ms}ms, "
            f"collect_surfaces={et_collect_surfaces_ms}ms, "
            f"calc_stat={et_calc_stat_ms}ms) "
            f"({addr_str} {len(realizations)=} )"
        )

        return xtgeo_surf

    def _make_addr_str(self, real_num: int, name: str, attribute: str, date_str: Optional[str]) -> str:
        addr_str = f"R:{real_num}__N:{name}__A:{attribute}__D:{date_str}__I:{self._iteration_name}__C:{self._case_uuid}"
        return addr_str


def _compute_statistical_surface(statistic: StatisticFunction, surface_coll: SurfaceCollection) -> xtgeo.RegularSurface:
    xtgeo_surf: xtgeo.RegularSurface = None
    if statistic == StatisticFunction.MIN:
        xtgeo_surf = surface_coll.min()
    elif statistic == StatisticFunction.MAX:
        xtgeo_surf = surface_coll.max()
    elif statistic == StatisticFunction.MEAN:
        xtgeo_surf = surface_coll.mean()
    elif statistic == StatisticFunction.P10:
        xtgeo_surf = surface_coll.p10()
    elif statistic == StatisticFunction.P90:
        xtgeo_surf = surface_coll.p90()
    elif statistic == StatisticFunction.P50:
        xtgeo_surf = surface_coll.p50()
    elif statistic == StatisticFunction.STD:
        xtgeo_surf = surface_coll.std()
    else:
        raise ValueError("Unhandled statistic function")

    return xtgeo_surf
