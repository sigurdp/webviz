import os
import logging
import datetime
import hashlib
import msgpack
from typing import TypeVar, Literal

import redis.asyncio as redis
from webviz_pkg.core_utils.perf_metrics import PerfMetrics
from azure.storage.blob.aio import BlobServiceClient, ContainerClient, StorageStreamDownloader, BlobClient
from azure.storage.blob import ContentSettings
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
from azure.storage.blob import ContainerClient as SyncContainerClient
from pydantic import BaseModel
from collections.abc import AsyncIterator

from contextlib import asynccontextmanager

from .authenticated_user import AuthenticatedUser


_REDIS_KEY_PREFIX = "temp_user_store_index"
_BLOB_CONTAINER_NAME = "test-user-scoped-temp-storage"


LOGGER = logging.getLogger(__name__)

PydanticModelType = TypeVar("PydanticModelType", bound=BaseModel)


class TempUserStoreFactory:
    _instance = None

    def __init__(
        self,
        redis_url: str,
        storage_account_conn_str: str,
        ttl_s: int,
        shared_redis_client: redis.Redis | None,
        shared_container_client: ContainerClient | None,
    ):
        self._redis_url: str = redis_url
        self._storage_account_conn_str: str = storage_account_conn_str
        self._ttl_s: int = ttl_s
        self._shared_redis_client: redis.Redis | None = shared_redis_client
        self._shared_container_client: ContainerClient | None = shared_container_client

    @classmethod
    def initialize(cls, use_shared_clients: bool, redis_url: str, storage_account_conn_str: str, ttl_s: int) -> None:
        if cls._instance is not None:
            raise RuntimeError("TempUserStoreFactory is already initialized")

        # Do a hard fail if the storage container does not exist
        # We don't create the container automatically since the container will require manual
        # setup anyways in order to configure lifecycle policies.
        if not _check_if_blob_container_exists(storage_account_conn_str, _BLOB_CONTAINER_NAME):
            raise RuntimeError(f"Blob container specified for TempUserStore does not exist: {_BLOB_CONTAINER_NAME}")

        shared_redis_client: redis.Redis | None = None
        shared_container_client: ContainerClient | None = None

        if use_shared_clients:
            # Use shared Redis and Blob Storage clients
            # This is the most optimal configuration, particularly for Redis, but it will not work in all our
            # scenarios, particularly when wrapping asynchronous code and in Celery using asyncio.run()
            shared_redis_client = redis.Redis.from_url(redis_url, decode_responses=True)
            shared_container_client = ContainerClient.from_connection_string(
                storage_account_conn_str, _BLOB_CONTAINER_NAME
            )

        cls._instance = cls(
            redis_url=redis_url,
            storage_account_conn_str=storage_account_conn_str,
            ttl_s=ttl_s,
            shared_redis_client=shared_redis_client,
            shared_container_client=shared_container_client,
        )

    @classmethod
    def get_instance(cls) -> "TempUserStoreFactory":
        if cls._instance is None:
            raise RuntimeError("TempUserStoreFactory is not initialized, call initialize() first")
        return cls._instance

    def get_store_for_user_id(self, user_id: str) -> "TempUserStore":
        if not user_id:
            raise ValueError("A user_id must be specified")

        return TempUserStore(
            user_id=user_id,
            redis_url=self._redis_url,
            storage_account_conn_str=self._storage_account_conn_str,
            ttl_s=self._ttl_s,
            shared_redis_client=self._shared_redis_client,
            shared_container_client=self._shared_container_client,
        )


class TempUserStore:
    """
    The class provides a temporary user-scoped storage system using Redis and Azure Blob Storage.

    Redis is used for fast indexing and lookups, while Azure Blob Storage is used for the actual data payloads.

    Each user gets their own storage area to prevent data leakage between users.

    This is a temporary storage solution with automatic expiration (set by a ttl in seconds). The TTL determines
    when the Redis entries expire, but does not affect the actual Blob data. Cleanup of blob data must be managed
    separately by using Azure Blob Storage lifecycle policies or manual deletion.

    It is very important that the lifetime of the blob data **must** be longer than the specified TTL. Otherwise, we
    risk having Redis entries pointing to blobs that no longer exist, which would lead to cache misses and errors.

    The current assumption is that TTL is in the order of hours, which would mean that a lifecycle policy that
    deletes blobs older than a day or two would be appropriate.
    """

    def __init__(
        self,
        user_id: str,
        redis_url: str,
        storage_account_conn_str: str,
        ttl_s: int,
        shared_redis_client: redis.Redis | None,
        shared_container_client: ContainerClient | None,
    ):
        if not user_id:
            raise ValueError("A user_id must be specified")

        self._user_id = user_id
        self._redis_url: str = redis_url
        self._storage_account_conn_str: str = storage_account_conn_str
        self._ttl_s: int = ttl_s
        self._shared_redis_client: redis.Redis | None = shared_redis_client
        self._shared_container_client: ContainerClient | None = shared_container_client

    async def put_bytes(self, key: str, payload: bytes, blob_prefix: str | None, blob_extension: str) -> bool:
        perf_metrics = PerfMetrics()

        blob_name = self._make_full_blob_name_from_payload(payload, blob_prefix, blob_extension)
        perf_metrics.record_lap("make-blob-name")

        async with self._goc_container_client() as container_client:
            await _upload_or_refresh_blob_metadata(container_client, blob_name, payload)
        perf_metrics.record_lap("upload-blob")

        redis_key = self._make_full_redis_key(key)
        async with self._goc_redis_client_async() as redis_client:
            await redis_client.setex(redis_key, self._ttl_s, blob_name)

        perf_metrics.record_lap("write-redis")

        size_mb = len(payload) / (1024 * 1024)
        LOGGER.debug(f"##### put_bytes() with payload of {size_mb:.2f}MB took: {perf_metrics.to_string()}")

        return True

    async def get_bytes(self, key: str) -> bytes | None:
        perf_metrics = PerfMetrics()

        redis_key = self._make_full_redis_key(key)
        LOGGER.debug(f"##### get_bytes() {redis_key=}")

        async with self._goc_redis_client_async() as redis_client:
            blob_name = await redis_client.get(redis_key)
        perf_metrics.record_lap("read-redis")

        if not blob_name:
            LOGGER.debug(f"##### get_bytes() cache miss took: {perf_metrics.to_string()}  [{key=}]")
            return None

        async with self._goc_container_client() as container_client:
            payload_bytes = await _download_blob(container_client, blob_name)
        perf_metrics.record_lap("download-blob")

        if not payload_bytes:
            LOGGER.debug(f"##### get_bytes() blob miss took: {perf_metrics.to_string()}  [{key=}, {blob_name=}]")
            return None

        size_mb = len(payload_bytes) / (1024 * 1024)
        LOGGER.debug(f"##### get_bytes() with with payload of {size_mb:.2f}MB took: {perf_metrics.to_string()}")

        return payload_bytes

    async def put_pydantic_model(
        self, key: str, model: BaseModel, format: Literal["msgpack", "json"], blob_prefix: str | None
    ) -> bool:
        perf_metrics = PerfMetrics()

        payload: bytes
        blob_extension: str
        if format == "msgpack":
            payload = _pydantic_to_msgpack(model)
            blob_extension = "msgpack"
        elif format == "json":
            payload = model.model_dump_json().encode("utf-8")
            blob_extension = "json"
        else:
            raise ValueError(f"Unsupported serialization format: {format}")

        perf_metrics.record_lap("serialize")

        ret_val = await self.put_bytes(key, payload, blob_prefix=blob_prefix, blob_extension=blob_extension)
        perf_metrics.record_lap("put-bytes")

        size_mb = len(payload) / (1024 * 1024)
        LOGGER.debug(f"##### put_pydantic() with payload of {size_mb:.2f}MB took: {perf_metrics.to_string()}")

        return ret_val

    async def get_pydantic_model(
        self, key: str, model_class: type[PydanticModelType], format: Literal["msgpack", "json"]
    ) -> PydanticModelType | None:
        perf_metrics = PerfMetrics()

        payload = await self.get_bytes(key)
        perf_metrics.record_lap("get-bytes")
        if not payload:
            LOGGER.debug(f"##### get_pydantic() cache miss took: {perf_metrics.to_string()}  [{key=}]")
            return None

        try:
            if format == "msgpack":
                model = _msgpack_to_pydantic(model_class, payload)
            elif format == "json":
                model = model_class.model_validate_json(payload.decode("utf-8"))
            else:
                raise ValueError(f"Unsupported serialization format: {format}")
        except Exception as e:
            raise ValueError(f"Failed to deserialize model {key=}, {format=}: {e}") from e

        perf_metrics.record_lap("deserialize")

        size_mb = len(payload) / (1024 * 1024)
        LOGGER.debug(f"##### get_pydantic() with with payload of {size_mb:.2f}MB took: {perf_metrics.to_string()}")

        return model

    @asynccontextmanager
    async def _goc_redis_client_async(self) -> AsyncIterator[redis.Redis]:
        if self._shared_redis_client:
            yield self._shared_redis_client
        else:
            client = redis.Redis.from_url(self._redis_url, decode_responses=True)
            try:
                yield client
            finally:
                await client.aclose()

    @asynccontextmanager
    async def _goc_container_client(self) -> AsyncIterator[ContainerClient]:
        if self._shared_container_client:
            yield self._shared_container_client
        else:
            client = ContainerClient.from_connection_string(self._storage_account_conn_str, _BLOB_CONTAINER_NAME)
            try:
                yield client
            finally:
                await client.close()

    def _make_full_redis_key(self, key: str) -> str:
        return f"{_REDIS_KEY_PREFIX}:user:{self._user_id}:{key}"

    def _make_full_blob_name_from_payload(self, payload: bytes, blob_prefix: str | None, extension: str) -> str:
        payload_hash = _compute_payload_hash(payload)
        if blob_prefix:
            return f"user__{self._user_id}/{blob_prefix}---sha__{payload_hash}.{extension}"
        else:
            return f"user__{self._user_id}/sha__{payload_hash}.{extension}"


def _pydantic_to_msgpack(model: BaseModel) -> bytes:
    return msgpack.packb(model.model_dump(), use_bin_type=True)


def _msgpack_to_pydantic(model_class: type[PydanticModelType], data: bytes) -> PydanticModelType:
    return model_class(**msgpack.unpackb(data, raw=False))


def _compute_payload_hash(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


async def _upload_or_refresh_blob_metadata(container_client: ContainerClient, blob_name: str, payload: bytes) -> str:
    blob_client: BlobClient = container_client.get_blob_client(blob_name)
    try:
        # Note that even if we're specifying `overwrite=False`, we might actually end up uploading the entire blob
        # payload here before that condition is detected. We should probably just check for the blob's existence first,
        # which is probably much more lightweight, even if it costs us an extra round trip.
        ret_dict = await blob_client.upload_blob(
            payload,
            overwrite=False,
            metadata={"refreshedAt": "never"},
            content_settings=ContentSettings(content_type="application/octet-stream"),
        )
        LOGGER.debug(f"##### _upload_or_refresh_blob_metadata() OK")
    except ResourceExistsError as e:
        # To stop the blob from being deleted by our lifecycle policy, we want to update the blob's modified time,
        # so we will set the a dummy metadata field to "refresh" and update the modified time stamp

        # Need to get existing metadata if we don't want to overwrite it
        # LOGGER.debug(f"##### _upload_or_refresh_blob_metadata() ResourceExistsError {e=}")
        # existing_blob_properties = await blob_client.get_blob_properties()
        # metadata = existing_blob_properties.metadata
        # metadata["refreshedAt"] = datetime.utcnow().isoformat())

        metadata = {"refreshedAt": datetime.datetime.utcnow().isoformat()}
        await blob_client.set_blob_metadata(metadata)
        LOGGER.debug(f"##### _upload_or_refresh_blob_metadata() REFRESHED")

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


def _check_if_blob_container_exists(storage_account_conn_string: str, container_name: str) -> bool:
    with SyncContainerClient.from_connection_string(storage_account_conn_string, container_name) as sync_client:
        try:
            sync_client.get_container_properties()
            return True
        except ResourceNotFoundError:
            return False
        except Exception as e:
            LOGGER.error(f"Failed to check if blob container exists: {e}")
            return False


def get_temp_user_store_for_user(authenticated_user: AuthenticatedUser) -> TempUserStore:
    """
    Convenience function to get a TempUserStore instance for the specified authenticated user.
    """
    factory = TempUserStoreFactory.get_instance()
    return factory.get_store_for_user_id(authenticated_user.get_user_id())


def get_temp_user_store_for_user_id(user_id: str) -> TempUserStore:
    factory = TempUserStoreFactory.get_instance()
    return factory.get_store_for_user_id(user_id)
