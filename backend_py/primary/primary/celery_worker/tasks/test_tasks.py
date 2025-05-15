import logging
import asyncio
import uuid
import io
import os
import msgpack

from azure.storage.blob import BlobServiceClient, ContainerClient, ContentSettings
from dotenv import load_dotenv
from pydantic import BaseModel

import xtgeo

from webviz_pkg.core_utils.perf_metrics import PerfMetrics

from primary.services.utils.otel_span_tracing import otel_span_decorator, start_otel_span, start_otel_span_async

from primary.celery_worker.celery_app import celery_app

from primary.services.sumo_access.surface_access import SurfaceAccess

from primary.routers.surface.surface_address import RealizationSurfaceAddress, ObservedSurfaceAddress, StatisticalSurfaceAddress
from primary.routers.surface.surface_address import decode_surf_addr_str
from primary.routers.surface.converters import to_api_surface_data_float


LOGGER = logging.getLogger(__name__)


load_dotenv()

AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
CONTAINER_NAME = "celery-results"

blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
container_client: ContainerClient = blob_service_client.get_container_client(CONTAINER_NAME)

try:
    container_client.create_container()
except Exception:
    # Container probably already exists
    pass  


def pydantic_to_msgpack(model: BaseModel) -> bytes:
    return msgpack.packb(model.model_dump(), use_bin_type=True)

def msgpack_to_pydantic(model_class: type[BaseModel], data: bytes) -> BaseModel:
    return model_class(**msgpack.unpackb(data, raw=False))




@celery_app.task
def capitalize_string(input_str: str) -> str:
    LOGGER.info(f"capitalize_string --- : {input_str=}")

    ret_str = input_str
    ret_str.capitalize()

    return f"Processed: {ret_str=}"


@celery_app.task
def surface_meta(sumo_access_token: str, case_uuid:str, ensemble_name: str) -> str:
    LOGGER.info(f"surface_meta --- : {sumo_access_token=}, {case_uuid=}, {ensemble_name=}")

    access = SurfaceAccess.from_iteration_name(sumo_access_token, case_uuid, ensemble_name)

    sumo_surf_meta_set = asyncio.run(access.get_realization_surfaces_metadata_async())

    LOGGER.info(sumo_surf_meta_set)

    return f"surface_meta OK"


@otel_span_decorator()
async def do_async_work(sumo_access_token: str, surf_addr_str: str) -> str:
    LOGGER.info(f"do_async_work --- : {surf_addr_str=}")

    perf_metrics = PerfMetrics()

    addr = decode_surf_addr_str(surf_addr_str)
    LOGGER.info(f"decoded addr: {addr=}")

    if addr.address_type != "REAL":
        raise ValueError(f"Unsupported address type: {addr.address_type}")
    

    access = SurfaceAccess.from_iteration_name(sumo_access_token, addr.case_uuid, addr.ensemble_name)
    xtgeo_surf: xtgeo.RegularSurface = await access.get_realization_surface_data_async(
        real_num=addr.realization,
        name=addr.name,
        attribute=addr.attribute,
        time_or_interval_str=addr.iso_time_or_interval,
    )
    perf_metrics.record_lap("get-xtgeo")
    LOGGER.info(f"xtgeo_surf: {xtgeo_surf=}")

    blob_name_without_extension = str(uuid.uuid4())

    gri_blob_name = blob_name_without_extension + ".gri"
    # xtg_blob_name = blob_name_without_extension + ".xtg"
    msgpack_blob_name = blob_name_without_extension + ".msgpack"

    byte_stream = io.BytesIO()
    xtgeo_surf.to_file(byte_stream, fformat="irap_binary")
    byte_stream.seek(0)
    container_client.upload_blob(name=gri_blob_name, data=byte_stream, overwrite=True)
    perf_metrics.record_lap("store-irap")

    # byte_stream = io.BytesIO()
    # xtgeo_surf.to_file(byte_stream, fformat="xtgregsurf")
    # byte_stream.seek(0)
    # container_client.upload_blob(name=xtg_blob_name, data=byte_stream, overwrite=True)

    surf_data_response = to_api_surface_data_float(xtgeo_surf)
    msgpacked_bytes = pydantic_to_msgpack(surf_data_response)

    content_settings = ContentSettings(content_type="application/vnd.msgpack")
    container_client.upload_blob(name=msgpack_blob_name, data=msgpacked_bytes, content_settings=content_settings, overwrite=True)
    perf_metrics.record_lap("store-msgpack")

    LOGGER.debug(f"do_async_work took: {perf_metrics.to_string()}")

    return blob_name_without_extension



@celery_app.task
def surface_from_addr(sumo_access_token: str, surf_addr_str: str) -> str:
    LOGGER.info(f"surface_from_addr --- : {surf_addr_str=}")

    blob_name = asyncio.run(do_async_work(sumo_access_token, surf_addr_str))

    LOGGER.info(f"surface_from_addr done, {blob_name=}")

    return f"surface_from_addr OK"

