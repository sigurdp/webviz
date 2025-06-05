import os
import logging
import datetime
import hashlib
import msgpack
from typing import Type, Literal

import redis.asyncio as redis
from dotenv import load_dotenv
from aiocache import BaseCache, RedisCache
from aiocache.serializers import MsgPackSerializer, PickleSerializer, StringSerializer
from redis.asyncio.connection import ConnectKwargs, parse_url
from webviz_pkg.core_utils.perf_metrics import PerfMetrics
from azure.storage.blob.aio import BlobServiceClient, ContainerClient, StorageStreamDownloader, BlobClient
from azure.storage.blob import ContentSettings
from azure.core.exceptions import ResourceExistsError
from pydantic import BaseModel

from primary import config

from .authenticated_user import AuthenticatedUser


LOGGER = logging.getLogger(__name__)

load_dotenv()
AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")

BLOB_CONTAINER_NAME = "test-user-scoped-temp-storage"

REDIS_NAMESPACE = "userScopedTempStorageIndex"
REDIS_TTL = 20 # 20 seconds for testing



class UserScopedTempStorage:
    def __init__(self, aio_cache: BaseCache, container_client: ContainerClient, authenticated_user: AuthenticatedUser):
        self._cache = aio_cache
        self._container_client = container_client
        self._authenticated_user_id = authenticated_user.get_user_id()

    async def put_bytes(self, key: str, payload: bytes, blob_extension: str) -> bool:
        perf_metrics = PerfMetrics()

        #blob_name = self._make_full_blob_name_from_payload(payload, blob_extension)
        blob_name = self._make_full_blob_name(key, blob_extension)
        perf_metrics.record_lap("make-blob-name")

        await _upload_or_refresh_blob(self._container_client, blob_name, payload)
        perf_metrics.record_lap("upload-blob")

        redis_key = self._make_full_redis_key(key)
        await self._cache.set(redis_key, blob_name)
        perf_metrics.record_lap("write-redis")

        size_mb = len(payload) / (1024 * 1024)
        LOGGER.debug(f"##### put_bytes() with payload of {size_mb:.2f}MB took: {perf_metrics.to_string()}")

        return True

    async def get_bytes(self, key: str) -> bytes | None:
        perf_metrics = PerfMetrics()

        redis_key = self._make_full_redis_key(key)
        LOGGER.debug(f"##### get_bytes() {redis_key=}")

        blob_name = await self._cache.get(redis_key)
        perf_metrics.record_lap("read-redis")

        if not blob_name:
            LOGGER.debug(f"##### get_bytes() cache miss took: {perf_metrics.to_string()}  [{key=}]")
            return None

        payload_bytes = await _download_blob(self._container_client, blob_name)
        perf_metrics.record_lap("download-blob")
        if not payload_bytes:
            LOGGER.debug(f"##### get_bytes() blob miss took: {perf_metrics.to_string()}  [{key=}, {blob_name=}]")
            return None

        size_mb = len(payload_bytes) / (1024 * 1024)
        LOGGER.debug(f"##### get_bytes() with with payload of {size_mb:.2f}MB took: {perf_metrics.to_string()}")

        return payload_bytes

    async def put_pydantic(self, key: str, pydantic_model: BaseModel, serialize_as: Literal["msgpack", "json"]) -> bool:
        perf_metrics = PerfMetrics()

        blob_extension: str
        if serialize_as == "msgpack":
            payload: bytes = _pydantic_to_msgpack(pydantic_model)
            blob_extension = "msgpack"
        elif serialize_as == "json":
            payload: bytes = pydantic_model.model_dump_json().encode("utf-8")
            blob_extension = "json"
        else:
            raise ValueError(f"Unsupported serialize_as value: {serialize_as}")
        
        perf_metrics.record_lap("serialize")

        ret_val = await self.put_bytes(key, payload, blob_extension=blob_extension)
        perf_metrics.record_lap("put-bytes")

        size_mb = len(payload) / (1024 * 1024)
        LOGGER.debug(f"##### put_pydantic() with payload of {size_mb:.2f}MB took: {perf_metrics.to_string()}")

        return ret_val

    async def get_pydantic(self, model_class: Type[BaseModel], key: str, serialized_as: Literal["msgpack", "json"]) -> BaseModel | None:
        perf_metrics = PerfMetrics()

        payload = await self.get_bytes(key)
        perf_metrics.record_lap("get-bytes")
        if not payload:
            LOGGER.debug(f"##### get_pydantic() cache miss took: {perf_metrics.to_string()}  [{key=}]")
            return None

        try:
            if serialized_as == "msgpack":
                model = _msgpack_to_pydantic(model_class, payload)
            elif serialized_as == "json":
                model = model_class.model_validate_json(payload.decode("utf-8"))
            else:
                raise ValueError(f"Unsupported serialized_as value: {serialized_as}")
            
            perf_metrics.record_lap("deserialize")
        except Exception as e:
            LOGGER.debug(
                f"##### get_pydantic() deserialize failed took: {perf_metrics.to_string()}  [{key=}]"
            )
            return None

        size_mb = len(payload) / (1024 * 1024)
        LOGGER.debug(f"##### get_pydantic() with with payload of {size_mb:.2f}MB took: {perf_metrics.to_string()}")

        return model

    def _make_full_redis_key(self, key: str) -> str:
        return f"user:{self._authenticated_user_id}:{key}"

    def _make_full_blob_name(self, key: str, extension: str) -> str:
        return f"user__{self._authenticated_user_id}/key__{key}.{extension}"

    def _make_full_blob_name_from_payload(self, payload: bytes, extension: str) -> str:
        # We could remove the user id from the blob name to save space which will de-dupe the blob payloads
        return f"user__{self._authenticated_user_id}/payload__{_compute_payload_hash(payload)}.{extension}"


def _pydantic_to_msgpack(model: BaseModel) -> bytes:
    return msgpack.packb(model.model_dump(), use_bin_type=True)


def _msgpack_to_pydantic(model_class: type[BaseModel], data: bytes) -> BaseModel:
    return model_class(**msgpack.unpackb(data, raw=False))


def _compute_payload_hash(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


async def _upload_or_refresh_blob(container_client: ContainerClient, blob_key: str, payload: bytes) -> str:
    blob_client: BlobClient = container_client.get_blob_client(blob_key)

    try:
        ret_dict = await blob_client.upload_blob(
            payload,
            overwrite=False,
            metadata={"refreshedAt": "never"},
            content_settings=ContentSettings(content_type="application/octet-stream"),
        )
        LOGGER.debug(f"##### _upload_or_refresh_blob() OK {ret_dict=}")
    except ResourceExistsError as e:
        # To stop the blob from being deleted by our lifecycle policy, we want to update the blob's modified time,
        # so we will set the a dummy metadata field to "refresh" and update the modified time stamp

        # Need to get existing metadata if we don't want to overwrite it
        # LOGGER.debug(f"##### _upload_or_refresh_blob() ResourceExistsError {e=}")
        # existing_blob_properties = await blob_client.get_blob_properties()
        # metadata = existing_blob_properties.metadata
        # metadata["refreshedAt"] = datetime.utcnow().isoformat())
        
        metadata =  {"refreshedAt": datetime.datetime.utcnow().isoformat()}
        await blob_client.set_blob_metadata(metadata)
        LOGGER.debug(f"##### _upload_or_refresh_blob() REFRESHED {metadata=}")

    return blob_client.url


async def _download_blob(container_client: ContainerClient, blob_name: str) -> bytes | None:
    blob_client = container_client.get_blob_client(blob_name)
    try:
        stream_downloader: StorageStreamDownloader[bytes] = await blob_client.download_blob(max_concurrency=8)
        payload = await stream_downloader.readall()
        return payload
    except Exception as e:
        LOGGER.debug(f"##### _download_blob() exception {e=}")
        return None


_redis_url_options: ConnectKwargs = parse_url(config.REDIS_CACHE_URL)

_aio_cache = RedisCache(
    endpoint=_redis_url_options["host"],
    port=_redis_url_options["port"],
    serializer=StringSerializer(),
    namespace=REDIS_NAMESPACE,
    ttl=REDIS_TTL
)

_blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
_container_client = _blob_service_client.get_container_client(BLOB_CONTAINER_NAME)


def _ensure_container_is_created():
    from azure.storage.blob import ContainerClient as SyncContainerClient

    sync_container_client = SyncContainerClient.from_connection_string(
        conn_str=AZURE_STORAGE_CONNECTION_STRING, container_name=BLOB_CONTAINER_NAME
    )

    try:
        sync_container_client.create_container()
    except Exception:
        pass  # Container probably already exists
    finally:
        sync_container_client.close()


_ensure_container_is_created()


def get_user_scoped_temp_storage(authenticated_user: AuthenticatedUser) -> UserScopedTempStorage:
    return UserScopedTempStorage(_aio_cache, _container_client, authenticated_user)
