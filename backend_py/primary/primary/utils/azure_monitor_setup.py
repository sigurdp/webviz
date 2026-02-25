from __future__ import annotations
import logging

from azure.monitor.opentelemetry import configure_azure_monitor
from fastapi import FastAPI
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
from opentelemetry.instrumentation.redis import RedisInstrumentor
from opentelemetry.sdk.resources import Resource


from dataclasses import dataclass
import os

from typing import Mapping, Optional

LOGGER = logging.getLogger(__name__)

@dataclass(frozen=True, kw_only=True)
class AzureMonitorSettings:
    insights_connection_string: str
    resource_attributes: Mapping[str, str]

    @classmethod
    def from_radix_env(cls) -> AzureMonitorSettings | None:
        service_name = os.getenv("RADIX_COMPONENT")
        service_namespace = os.getenv("RADIX_ENVIRONMENT")
        if not service_name or not service_namespace:
            # These environment variables are expected to be set in all Radix environments, so log an error if they are missing,
            # but return None to allow the application to continue running without telemetry instead of crashing.
            LOGGER.error("RADIX_COMPONENT and/or RADIX_ENVIRONMENT env variables are not set as expected")
            return None

        full_git_commit_hash = os.getenv("RADIX_GIT_COMMIT_HASH")
        if full_git_commit_hash:
            # Shorten to 7 characters for better readability
            service_version = full_git_commit_hash[:7]
        else:
            service_version = "NA"

        # The service_name retrieved from radix will typically be lowercase prod, preprod, dev or review.
        # Convert to uppercase and construct the expected environment variable name for the connection string
        # on the form "WEBVIZ_INSIGHTS_CONNECTIONSTRING_<NAMESPACE>"", e.g. "WEBVIZ_INSIGHTS_CONNECTIONSTRING_PROD"
        env_var_name = f"WEBVIZ_INSIGHTS_CONNECTIONSTRING_{service_namespace.upper()}"
        insights_connection_string = os.getenv(env_var_name)
        if not insights_connection_string:
            # Currently we may not have environment variables set up for all Radix environments, so log a warning if the expected environment variable is missing,
            # but return None to allow the application to continue running without telemetry instead of crashing.
            LOGGER.warning(f"Cannot resolve Azure Monitor telemetry settings, {env_var_name} env variable is not set")
            return None

        return AzureMonitorSettings(
            insights_connection_string=insights_connection_string,
            resource_attributes={
                "service.name": f"new_{service_name}",
                "service.namespace": f"new_{service_namespace}",
                "service.version": f"new_{service_version}",
            },
        )

    @classmethod
    def for_local_dev(cls, service_name: str) -> AzureMonitorSettings | None:
        connection_string = os.getenv("APPLICATIONINSIGHTS_CONNECTION_STRING")
        if not connection_string:
            return None

        return cls(
            insights_connection_string=connection_string,
            resource_attributes={
                "service.name": f"new_{service_name}",
                "service.namespace": "new_local",
                "service.version": "new_NA",
            },
        )


def is_on_radix_platform() -> bool:
    return os.getenv("RADIX_APP") is not None


# A lot of the configuration will be read from environment variables during the execution of this function.
# Notable environment variables that may be consumed are:
# - APPLICATIONINSIGHTS_CONNECTION_STRING
# - OTEL_RESOURCE_ATTRIBUTES
# - OTEL_SERVICE_NAME
def setup_azure_monitor_telemetry_for_primary(fastapi_app: FastAPI) -> None:
    if is_on_radix_platform():
        monitor_settings = AzureMonitorSettings.from_radix_env()
    else:
        # For local development and testing of telemetry.
        # Picks up APPLICATIONINSIGHTS_CONNECTION_STRING env variable if it is set and configure settings from that.
        monitor_settings = AzureMonitorSettings.for_local_dev(service_name="backend-primary")

    LOGGER.info("------------------- Azure Monitor Telemetry Settings ------------------")
    LOGGER.info(monitor_settings)
    LOGGER.info("-----------------------------------------------------------------------")
    if not monitor_settings:
        LOGGER.warning("Skipping telemetry configuration for primary backend, could not determine settings")
        return

    LOGGER.info("Configuring Azure Monitor telemetry for primary backend")

    # Note that this call will throw an exception if the APPLICATIONINSIGHTS_CONNECTION_STRING
    # environment variable is not set or if it is invalid.
    # Starting with version 1.8.6, the default sampler is RateLimitedSampler. We restore the old behavior by setting the
    # sampling_ratio to 1.0, which restores classic Application Insights sampler.

    configure_azure_monitor(
        connection_string=monitor_settings.insights_connection_string,
        resource=Resource.create(attributes=monitor_settings.resource_attributes),
        sampling_ratio=1.0,
        logging_formatter=logging.Formatter("[%(name)s]: %(message)s"),
        instrumentation_options={
            "django": {"enabled": False},
            "flask": {"enabled": False},
            "psycopg2": {"enabled": False},
        },
    )

    FastAPIInstrumentor.instrument_app(fastapi_app)

    HTTPXClientInstrumentor().instrument()

    # Should we keep Redis instrumented or does it generate more noise than insights?
    RedisInstrumentor().instrument()
