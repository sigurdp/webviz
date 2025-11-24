import asyncio
import logging
from typing import Annotated

import httpx
import starsessions
from fastapi import APIRouter, Depends, HTTPException, Request, Response, status, Query, Path
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

from webviz_services.graph_access.graph_access import GraphApiAccess
from webviz_services.utils.task_meta_tracker import get_task_meta_tracker_for_user

from primary.auth.auth_helper import AuthenticatedUser, AuthHelper
from primary.middleware.add_browser_cache import no_cache
from primary.utils.response_perf_metrics import ResponsePerfMetrics

LOGGER = logging.getLogger(__name__)


class UserInfo(BaseModel):
    username: str
    display_name: str | None = None
    avatar_b64str: str | None = None
    has_sumo_access: bool
    has_smda_access: bool


router = APIRouter()


@router.post("/logout")
async def post_logout(request: Request) -> str:
    await starsessions.load_session(request)
    request.session.clear()

    return "Logout OK"


@router.get("/profile", response_model=UserInfo)
@no_cache
async def get_my_profile(
    request: Request,
    include_graph_api_info: bool = Query(
        False, description="Set to true to include user avatar and display name from Microsoft Graph API"
    ),
) -> UserInfo:
    """
    Get the user profile of the currently logged in user
    """
    await starsessions.load_session(request)
    authenticated_user = AuthHelper.get_authenticated_user(request)

    if not authenticated_user:
        # What is the most appropriate return code?
        # Probably 401, but seemingly we got into trouble with that. Should try again
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="No user is logged in")

    user_info = UserInfo(
        username=authenticated_user.get_username(),
        avatar_b64str=None,
        display_name=None,
        has_sumo_access=authenticated_user.has_sumo_access_token(),
        has_smda_access=authenticated_user.has_smda_access_token(),
    )

    if authenticated_user.has_graph_access_token() and include_graph_api_info:
        graph_api_access = GraphApiAccess(authenticated_user.get_graph_access_token())
        try:
            avatar_b64str_future = asyncio.create_task(graph_api_access.get_user_profile_photo("me"))
            graph_user_info_future = asyncio.create_task(graph_api_access.get_user_info("me"))

            avatar_b64str = await avatar_b64str_future
            graph_user_info = await graph_user_info_future

            user_info.avatar_b64str = avatar_b64str
            if graph_user_info is not None:
                user_info.display_name = graph_user_info.get("displayName", None)
        except httpx.HTTPError as exc:
            LOGGER.error(f"Error while fetching user avatar and info from Microsoft Graph API (HTTP error):\n{exc}")
        except httpx.InvalidURL as exc:
            LOGGER.error(f"Error while fetching user avatar and info from Microsoft Graph API (Invalid URL):\n{exc}")

    return user_info


@router.post("/tasks/purge")
async def post_purge_all_tasks(
    response: Response,
    authenticated_user: Annotated[AuthenticatedUser, Depends(AuthHelper.get_authenticated_user)],
) -> str:
    perf_metrics = ResponsePerfMetrics(response)
    LOGGER.info(f"Purging all tasks for user {authenticated_user.get_username()}...")

    LOGGER.info("Purging tasks from task meta tracker")
    task_tracker = get_task_meta_tracker_for_user(authenticated_user)
    await task_tracker.purge_all_task_meta_async()

    LOGGER.info(f"Purging of tasks took: {perf_metrics.to_string()}")

    return "All tasks purged"


@router.post("/tasks/{task_id}/cancel")
async def post_cancel_task(
    authenticated_user: Annotated[AuthenticatedUser, Depends(AuthHelper.get_authenticated_user)],
    task_id: Annotated[str, Path(description="The task id to cancel")],
) -> str:
    raise HTTPException(status.HTTP_501_NOT_IMPLEMENTED)


@router.post("/caches/purge")
async def post_purge_all_caches(
    authenticated_user: Annotated[AuthenticatedUser, Depends(AuthHelper.get_authenticated_user)],
) -> str:
    raise HTTPException(status.HTTP_501_NOT_IMPLEMENTED)


# !!!!!
# !!!!!
# !!!!!
# Convenience GET endpoints for logout and purge all
# !!!!!
# !!!!!
# !!!!!
@router.get("/logout", response_class=HTMLResponse)
async def get_logout() -> str:
    html_content = """
    <!doctype html>
    <html>
    <head><meta charset="utf-8"/><title>Logging out…</title></head>
    <body>
        <form id="logoutForm" method="post" action="/api/me/logout">
            <p>Logging out...</p>
        </form>
        <script>
            setTimeout(() => {
                document.getElementById('logoutForm').submit();
            }, 2000);
        </script>
        <noscript>
            <p>Click the button to log out.</p>
            <button form="logoutForm" type="submit">Log out</button>
        </noscript>
    </body>
    </html>
    """

    return HTMLResponse(content=html_content, status_code=200)


@router.get("/caches/purge-now")
async def get_purge_all_caches_now(
    authenticated_user: Annotated[AuthenticatedUser, Depends(AuthHelper.get_authenticated_user)],
) -> str:
    raise HTTPException(status.HTTP_501_NOT_IMPLEMENTED)
