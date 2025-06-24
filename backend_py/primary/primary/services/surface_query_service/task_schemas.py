from typing import List, Literal

from pydantic import BaseModel


class RealizationSurfaceObject(BaseModel):
    realization: int
    objectUuid: str


class SampleInPointsTaskInput(BaseModel):
    userId: str
    sasToken: str
    blobStoreBaseUri: str
    realizationSurfaceObjects: List[RealizationSurfaceObject]
    xCoords: List[float]
    yCoords: List[float]
    targetStoreKey: str


class NamedPointSet(BaseModel):
    name: str
    xCoords: List[float]
    yCoords: List[float]
    targetStoreKey: str

class BatchSamplePointSetsTaskInput(BaseModel):
    userId: str
    sasToken: str
    blobStoreBaseUri: str
    realizationSurfaceObjects: List[RealizationSurfaceObject]
    pointSets: List[NamedPointSet]


class RealizationValues(BaseModel):
    realization: int
    sampledValues: list[float]


# This model is used to represent the result of a single sample surface in points task
# It is this data structure (in msgpack format) that is written to the TempUserStore
class SampleInPointsTaskResult(BaseModel):
    realizationSamples: List[RealizationValues]
    undefLimit: float


# This a general task status response from the Http Bridge of our go task service.
class TaskStatusResponse(BaseModel):
    taskId: str
    status: Literal["pending", "running", "success", "failure"]
    result: dict | None = None
    errorMsg: str | None = None
