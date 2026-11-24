import json
import logging
import secrets

from redis.asyncio import Redis

LOGGER = logging.getLogger(__name__)

_REDIS_KEY_PREFIX = "dispatch_token"
_DISPATCH_TOKEN_TTL_SECONDS = 4 * 3600  # Must cover expected queue wait, execution and retries.


class DispatchTokenStore:
    """
    Stores reusable, time-limited dispatch tokens in Redis, mapping each token to a session_id.

    Tokens remain valid until their TTL expires, allowing task retries and access token renewal.
    Lookups do not extend the TTL.

    Note that this class should only be used with the same Redis instance as the one already
    used for storing encrypted auth sessions since they share the same security boundary.
    """

    def __init__(self, redis_url: str) -> None:
        self._redis: Redis = Redis.from_url(redis_url)

    async def create_token_async(self, session_id: str) -> str:
        new_dispatch_token = secrets.token_urlsafe(32)
        data = json.dumps({"session_id": session_id})

        redis_key = f"{_REDIS_KEY_PREFIX}:{new_dispatch_token}"
        await self._redis.set(redis_key, data, ex=_DISPATCH_TOKEN_TTL_SECONDS)

        return new_dispatch_token

    async def get_session_id_async(self, dispatch_token: str) -> str | None:
        """
        Retrieve the associated session_id without extending its TTL.
        Returns None if the token is missing or expired.
        """
        redis_key = f"{_REDIS_KEY_PREFIX}:{dispatch_token}"
        raw: bytes | None = await self._redis.get(redis_key)
        if raw is None:
            LOGGER.warning("Dispatch token not found or expired")
            return None

        data = json.loads(raw)
        return data["session_id"]


class DispatchTokenStoreSingleton:
    _dispatch_token_store_instance: DispatchTokenStore | None = None

    @classmethod
    def initialize(cls, redis_url: str) -> None:
        if cls._dispatch_token_store_instance is not None:
            raise RuntimeError("DispatchTokenStoreSingleton is already initialized")
        cls._dispatch_token_store_instance = DispatchTokenStore(redis_url)

    @classmethod
    def get_instance(cls) -> DispatchTokenStore:
        if cls._dispatch_token_store_instance is None:
            raise RuntimeError("DispatchTokenStoreSingleton is not initialized, call initialize() first")
        return cls._dispatch_token_store_instance
