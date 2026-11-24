from dataclasses import dataclass

from webviz_services.utils.payload_encryption import PayloadEncryption

from .utils.sumo_access_token_fetcher import SumoAccessTokenFetcher


@dataclass(frozen=True)
class TaskServices:
    payload_encryption: PayloadEncryption
    sumo_access_token_fetcher: SumoAccessTokenFetcher
