import logging

from opentelemetry import trace
from starlette.requests import Request
from starlette.types import ASGIApp, Scope, Receive, Send

from webviz_services.utils.authenticated_user import AuthenticatedUser

LOGGER = logging.getLogger(__name__)


class OtelSpanClientAddressEnrichmentMiddleware:
    """
    Middleware that enriches OpenTelemetry spans with client address information from the request.

    Since we're normally running behind a proxy, this middleware must be placed after proxy middleware,
    in our case ProxyHeadersMiddleware, which populates the client address information from the X-Forwarded-For header.
    """

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        curr_span = trace.get_current_span()
        if curr_span.is_recording():
            request = Request(scope)
            if request.client:
                LOGGER.debug(f"-------------------- OtelSpanClientAddressEnrichmentMiddleware: {request.client.host=}")
                # curr_span.set_attribute("http.client_ip", request.client.host)      # legacy-ish but widely used
                # curr_span.set_attribute("net.peer.ip", request.client.host)         # also commonly recognized
                curr_span.set_attribute("client.address", request.client.host)  # newer semconv

                curr_span.set_attribute("app.client_ip_observed", request.client.host)
            else:
                LOGGER.warning("!!!!!!!!!!!!!!!!!!!!! Could not get client IP from request")

        await self.app(scope, receive, send)


class OtelSpanEndUserEnrichmentMiddleware:
    """
    Middleware that enriches OpenTelemetry spans with end user information from the request.
    """

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        curr_span = trace.get_current_span()
        if curr_span.is_recording():
            request = Request(scope)
            try:
                maybe_authenticated_user_obj = request.state.authenticated_user_obj
                if maybe_authenticated_user_obj and isinstance(maybe_authenticated_user_obj, AuthenticatedUser):

                    user_id = maybe_authenticated_user_obj.get_user_id()
                    user_name = maybe_authenticated_user_obj.get_username()

                    LOGGER.debug(f"-------------------- OtelSpanEndUserEnrichmentMiddleware: {user_id=}, {user_name=}")

                    # Shows up as "Auth Id", "Authenticated user Id" or user_AuthenticatedId in Application Insights
                    curr_span.set_attribute("enduser.id", f"auth_{user_name}")

                    # Shows up as "User Id" or user_Id in Application Insights
                    curr_span.set_attribute("enduser.pseudo.id", f"pseudo_{user_name}")

                    curr_span.set_attribute("app.user_name_raw", f"cust_{user_name}")
                    curr_span.set_attribute("app.user_id_raw", f"cust_{user_id}")

            except:  # nosec # pylint: disable=bare-except
                LOGGER.warning("!!!!!!!!!!!!!!!!!!!!! OtelSpanEndUserEnrichmentMiddleware: Could not get end user information from request")
                pass

        await self.app(scope, receive, send)
