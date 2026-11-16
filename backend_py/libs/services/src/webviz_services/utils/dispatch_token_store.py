import json
import logging
import secrets

from redis.asyncio import Redis

LOGGER = logging.getLogger(__name__)

_DISPATCH_TOKEN_PREFIX = "dispatch-token:"
_DISPATCH_TOKEN_TTL_SECONDS = 4 * 3600  # 4 hours — expected upper bound on queue wait time


class DispatchTokenStore:
    """
    Stores short-lived dispatch tokens in Redis, mapping each token to a session_id.

    Tokens are consumed atomically (read-and-delete) to prevent replay attacks.
    Kept in redis-user-session alongside the MSAL token caches, so the security
    boundary is the same — a Redis breach already exposes session_ids as key names.
    """

    def __init__(self, redis_url: str) -> None:
        self._redis: Redis = Redis.from_url(redis_url)

    def generate_token(self) -> str:
        return secrets.token_urlsafe(32)

    async def store(self, dispatch_token: str, session_id: str) -> None:
        data = json.dumps({"session_id": session_id})
        key = _DISPATCH_TOKEN_PREFIX + dispatch_token
        await self._redis.set(key, data, ex=_DISPATCH_TOKEN_TTL_SECONDS)

    async def consume(self, dispatch_token: str) -> str | None:
        """
        Atomically retrieve and delete the dispatch token entry.
        Returns session_id on success, None if not found or already consumed.
        """
        key = _DISPATCH_TOKEN_PREFIX + dispatch_token
        # GETDEL is atomic — prevents two concurrent workers from consuming the same token
        raw: bytes | None = await self._redis.getdel(key)
        if raw is None:
            LOGGER.warning("Dispatch token not found or already consumed")
            return None

        data = json.loads(raw)
        return data["session_id"]
