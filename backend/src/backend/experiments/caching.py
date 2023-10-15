import zlib
import io
import logging
from typing import Any
import xtgeo
import numpy as np
from src import config
from src.services.utils.perf_timer import PerfTimer
import redis.asyncio as redis
from aiocache import RedisCache, BaseCache
from aiocache.serializers import PickleSerializer
from aiocache.serializers import MsgPackSerializer
from aiocache.serializers import BaseSerializer
from src.services.utils.authenticated_user import AuthenticatedUser
from dataclasses import dataclass

LOGGER = logging.getLogger(__name__)


XTGEO_FILE_FORMAT = "irap_binary"
#XTGEO_FILE_FORMAT = "xtgregsurf"



class CompressedPickleSerializer(PickleSerializer):

    # This is needed because zlib works with bytes.
    # this way the underlying backend knows how to
    # store/retrieve values
    DEFAULT_ENCODING = None

    def __init__(self) -> None:
        super().__init__()
        self._coreSerializer = PickleSerializer()

    def dumps(self, value: Any) -> bytes | None:
        ser_value_bytes = self._coreSerializer.dumps(value)
        if ser_value_bytes is None:
            return None
        
        compressed = zlib.compress(ser_value_bytes)
        return compressed

    def loads(self, value: Any) -> Any | None:
        try:
            decompressed = zlib.decompress(value)
        except Exception as e:
            return None
        
        return self._coreSerializer.loads(decompressed)


@dataclass
class SurfMeta:
    ncol: int
    nrow: int
    xinc: float
    yinc: float
    xori: float
    yori: float
    yflip: int
    rotation: float


class UserCache:
    def __init__(self, aio_cache: BaseCache, authenticated_user: AuthenticatedUser):
        self._cache = aio_cache
        self.authenticated_user_id = authenticated_user.get_user_id()

    def _make_full_key(self, key: str) -> str:
        return f"user:{self.authenticated_user_id}:{key}"

    async def set_Any(self, key: str, obj: Any) -> bool:
        timer = PerfTimer()
        res = await self._cache.set(self._make_full_key(key), obj)
        LOGGER.debug(f"##### set_Any() took: {timer.elapsed_ms()}ms")
        return res
    
    async def get_Any(self, key: str) -> Any:
        timer = PerfTimer()
        res = await self._cache.get(self._make_full_key(key))
        LOGGER.debug(f"##### get_Any() data={'yes' if res is not None else 'no'} took: {timer.elapsed_ms()}ms")
        return res
    
    async def set_RegularSurface(self, key: str, surf: xtgeo.RegularSurface)-> bool:
        timer = PerfTimer()
        byte_io = io.BytesIO()
        surf.to_file(byte_io, fformat=XTGEO_FILE_FORMAT)
        byte_io.seek(0)
        the_bytes = byte_io.getvalue()
        res = await self._cache.set(self._make_full_key(key), the_bytes)
        LOGGER.debug(f"##### set_RegularSurface() ({(len(the_bytes)/1024):.2f}KB) took: {timer.elapsed_ms()}ms")
        return res

    async def get_RegularSurface(self, key: str) -> xtgeo.RegularSurface | None:
        timer = PerfTimer()

        cached_bytes = await self._cache.get(self._make_full_key(key))
        #print(f"{type(cached_bytes)=}")
        
        if cached_bytes is None:
            LOGGER.debug(f"##### get_RegularSurface() data=no took: {timer.elapsed_ms()}ms")
            return None
        
        try:
            surf = xtgeo.surface_from_file(io.BytesIO(cached_bytes), fformat=XTGEO_FILE_FORMAT)
        except Exception as e:
            LOGGER.debug(f"##### get_RegularSurface() data=convException took: {timer.elapsed_ms()}ms")
            return None
        
        if surf is None:
            LOGGER.debug(f"##### get_RegularSurface() data=convFailed took: {timer.elapsed_ms()}ms")
            return None

        LOGGER.debug(f"##### get_RegularSurface() data=yes ({(len(cached_bytes)/1024):.2f}KB) took: {timer.elapsed_ms()}ms")

        return surf

    async def set_RegularSurface_HACK(self, key: str, surf: xtgeo.RegularSurface)-> bool:
        timer = PerfTimer()

        surf_meta = SurfMeta(
            ncol=surf.ncol,
            nrow=surf.nrow,
            xinc=surf.xinc,
            yinc=surf.yinc,
            xori=surf.xori,
            yori=surf.yori,
            yflip=surf.yflip,
            rotation=surf.rotation,
        )

        values = surf.values;

        masked_values = surf.values.astype(np.float32)
        values_np = np.ma.filled(masked_values, fill_value=np.nan)
        arr_bytes = bytes(values_np.ravel(order="C").data)

        # print("----------------------------", flush=True)
        # print(f"{type(arr_bytes)=}", flush=True)
        # print("----------------------------", flush=True)

        pairs = []
        pairs.append([self._make_full_key(key + ":surf_meta"), surf_meta])
        pairs.append([self._make_full_key(key + ":surf_bytes"), arr_bytes])

        res = await self._cache.multi_set(pairs)
        LOGGER.debug(f"##### set_RegularSurface_HACK() ({(len(arr_bytes)/1024):.2f}KB) took: {timer.elapsed_ms()}ms")
        return res

    async def get_RegularSurface_HACK(self, key: str) -> xtgeo.RegularSurface | None:
        timer = PerfTimer()

        key_arr = []
        key_arr.append(self._make_full_key(key + ":surf_meta"))
        key_arr.append(self._make_full_key(key + ":surf_bytes"))

        items_arr = await self._cache.multi_get(key_arr)
        
        if items_arr is None or len(items_arr) != 2:
            LOGGER.debug(f"##### get_RegularSurface_HACK() data=no took: {timer.elapsed_ms()}ms")
            return None

        surf_meta = items_arr[0]
        arr_bytes = items_arr[1]
        if (surf_meta is None) or (arr_bytes is None):
            LOGGER.debug(f"##### get_RegularSurface_HACK() data=no took: {timer.elapsed_ms()}ms")
            return None
        
        # print("----------------------------", flush=True)
        # print(f"{surf_meta=}", flush=True)
        # print(f"{type(arr_bytes)=}", flush=True)
        # print("----------------------------", flush=True)

        values = np.frombuffer(arr_bytes, dtype=np.float32).reshape(surf_meta.nrow, surf_meta.ncol)

        try:
            surf = xtgeo.RegularSurface(
                ncol=surf_meta.ncol,
                nrow=surf_meta.nrow,
                xinc=surf_meta.xinc,
                yinc=surf_meta.yinc,
                xori=surf_meta.xori,
                yori=surf_meta.yori,
                yflip=surf_meta.yflip,
                rotation=surf_meta.rotation,
                values=values,
            )
        except Exception as e:
            LOGGER.debug(f"##### get_RegularSurface_HACK() data=convException took: {timer.elapsed_ms()}ms")
            return None

        LOGGER.debug(f"##### get_RegularSurface_HACK() data=yes ({(len(arr_bytes)/1024):.2f}KB) took: {timer.elapsed_ms()}ms")

        return surf



redis_client = redis.Redis.from_url(config.REDIS_URL)

#aio_cache = RedisCache(endpoint="redis", port=6379, namespace="comprCache3", serializer=CompressedPickleSerializer())
aio_cache = RedisCache(endpoint="redis-login-state", port=6379, namespace="uncomprCache", timeout=30, serializer=PickleSerializer())

def get_user_cache(authenticated_user: AuthenticatedUser) -> UserCache:
    return UserCache(aio_cache, authenticated_user)

