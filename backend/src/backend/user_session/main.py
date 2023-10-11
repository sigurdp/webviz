import logging

from fastapi import FastAPI
from fastapi.responses import ORJSONResponse

from src.backend.shared_middleware import add_shared_middlewares
from .inactivity_shutdown import InactivityShutdown
from .routers.general import router as general_router

# mypy: disable-error-code="attr-defined"
from .routers.grid.router import router as grid_router
from .routers.surface.router import router as surface_router

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s %(levelname)-3s [%(name)s]: %(message)s",
    datefmt="%H:%M:%S",
)
logging.getLogger("src.backend.user_session").setLevel(level=logging.DEBUG)
logging.getLogger("src.backend.experiments").setLevel(level=logging.DEBUG)
logging.getLogger("src.services.sumo_access.surface_access").setLevel(level=logging.DEBUG)

app = FastAPI(default_response_class=ORJSONResponse)

app.include_router(general_router)
app.include_router(grid_router, prefix="/grid")
app.include_router(surface_router, prefix="/surface")
add_shared_middlewares(app)

# We shut down the user session container after some
# minutes without receiving any new requests:
InactivityShutdown(app, inactivity_limit_minutes=30)

print("Succesfully completed user session server initialization.")
