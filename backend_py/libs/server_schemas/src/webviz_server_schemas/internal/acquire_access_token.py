from typing import Literal
from uuid import UUID

from pydantic import BaseModel


# Internal request schemas for acquiring access tokens
class AccessTokenRequest(BaseModel):
    #fmt: off
    request_id: UUID                # Unique identifier for the request
    dispatch_token: str             # Dispatch token used to authenticate the request with the internal endpoint
    target_service: Literal["sumo"] # The service for which the access token is being requested
    #fmt: on


class AccessTokenResponse(BaseModel):
    #fmt: off
    request_id: UUID                # Unique identifier for the request
    target_service: Literal["sumo"] # The service for which the access token was requested
    access_token: str               # The acquired access token
    expires_at: int                 # Unix timestamp (seconds since epoch UTC) indicating when the access token expires
    #fmt: on
