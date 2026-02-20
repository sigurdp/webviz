import base64
import logging
from typing import List

import starsessions
from fastapi import Request
from fastapi.responses import PlainTextResponse, RedirectResponse
from starlette.types import ASGIApp, Receive, Scope, Send
from webviz_core_utils.perf_metrics import PerfMetrics

from .auth_helper import AuthHelper

from opentelemetry import trace


LOGGER = logging.getLogger(__name__)


class EnforceLoggedInMiddleware:
    """
    Pure ASGI middleware to enforce that the user is logged in

    By default, all paths except `/login` and `/auth-callback` are protected.

    Additional paths can be left unprotected by specifying them in `unprotected_paths`

    By default all protected paths will return status code 401 if user is not logged in,
    but the `paths_redirected_to_login` can be used to specify a list of paths that
    should cause redirect to the `/login` endpoint instead.
    """

    def __init__(
        self,
        app: ASGIApp,
        unprotected_paths: List[str] | None = None,
        paths_redirected_to_login: List[str] | None = None,
    ):
        self._app = app
        self._unprotected_paths = unprotected_paths or []
        self._paths_redirected_to_login = paths_redirected_to_login or []

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            return await self._app(scope, receive, send)

        perf_metrics = PerfMetrics()

        path_to_check = scope.get("path", "")
        root_path = scope.get("root_path", "")
        if root_path:
            path_to_check = path_to_check.replace(root_path, "")

        path_is_protected = True
        if path_to_check in ["/login", "/auth-callback"] + self._unprotected_paths:
            path_is_protected = False

        if path_is_protected:
            request = Request(scope, receive)

            await starsessions.load_session(request)
            perf_metrics.record_lap("load-session")

            authenticated_user = AuthHelper.get_authenticated_user(request)
            perf_metrics.record_lap("get-auth-user")

            is_logged_in = authenticated_user is not None

            LOGGER.debug(
                f"EnforceLoggedInMiddleware for protected path ({path_to_check=}, {is_logged_in=}) took: {perf_metrics.to_string()}"
            )

            if not is_logged_in:
                if path_to_check in self._paths_redirected_to_login:
                    LOGGER.info("EnforceLoggedInMiddleware - redirecting to login page")
                    target_url_b64 = base64.urlsafe_b64encode(str(request.url).encode()).decode()
                    redirect_response = RedirectResponse(f"{root_path}/login?redirect_url_after_login={target_url_b64}")
                    await redirect_response(scope, receive, send)
                    return

                LOGGER.info("EnforceLoggedInMiddleware - user is not authorized yet")
                text_response = PlainTextResponse("Not authorized yet, must log in", 401)
                await text_response(scope, receive, send)
                return

            # !!!!!!!!!!!!!!!!!!!!!
            # !!!!!!!!!!!!!!!!!!!!!
            # !!!!!!!!!!!!!!!!!!!!!
            # !!!!!!!!!!!!!!!!!!!!!
            # !!!!!!!!!!!!!!!!!!!!!
            # !!!!!!!!!!!!!!!!!!!!!
            user_id = authenticated_user.get_user_id()
            user_name = authenticated_user.get_username()
            curr_span = trace.get_current_span()

            # Shows up as "Auth Id", "Authenticated user Id" or user_AuthenticatedId in Application Insights
            curr_span.set_attribute("enduser.id", f"auth_{user_name}")

            # Shows up as "User Id" or user_Id in Application Insights
            curr_span.set_attribute("enduser.pseudo.id", f"pseudo_{user_name}")

            curr_span.set_attribute("app.user_name_raw", f"cust_{user_name}")
            curr_span.set_attribute("app.user_id_raw", f"cust_{user_id}")

            # !!!!!!!!!!!!!!!!!!!!!
            # if request.client:
            #     LOGGER.info(f"!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! - setting client host in span attributes: {request.client.host}")
            #     curr_span.set_attribute("app.sigurd_ip", request.client.host)

            #     curr_span.set_attribute("http.client_ip", request.client.host)      # legacy-ish but widely used
            #     curr_span.set_attribute("net.peer.ip", request.client.host)         # also commonly recognized
            #     curr_span.set_attribute("client.address", request.client.host)      # newer semconv


        else:
            LOGGER.debug(
                f"EnforceLoggedInMiddleware for UNPROTECTED path ({path_to_check=}) took: {perf_metrics.to_string()}"
            )

        await self._app(scope, receive, send)
