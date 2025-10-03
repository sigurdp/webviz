import datetime
import logging

from fastapi import APIRouter

from primary.middleware.add_browser_cache import no_cache

LOGGER = logging.getLogger(__name__)


router = APIRouter()


@router.get("/alive")
@no_cache
async def get_alive() -> str:
    return f"ALIVE: Backend is alive at this time: {datetime.datetime.now()}"


@router.get("/alive_protected")
@no_cache
async def get_alive_protected() -> str:
    return f"ALIVE_PROTECTED: Backend is alive at this time: {datetime.datetime.now()}"
