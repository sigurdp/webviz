package tasks

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"surface_query/operations"
	"surface_query/utils"

	"github.com/vmihailenco/msgpack/v5"

	"github.com/go-playground/validator/v10"
	"github.com/hibiken/asynq"
)

type realizationSurfaceObject struct {
	Realization int    `json:"realization" binding:"required"`
	ObjectUuid  string `json:"objectUuid" binding:"required"`
}

type pointSet struct {
	Name           string    `json:"name" validate:"required"`
	XCoords        []float64 `json:"xCoords" validate:"required"`
	YCoords        []float64 `json:"yCoords" validate:"required"`
	TargetStoreKey string    `json:"targetStoreKey" validate:"required"`
}

type sampleInPointsTaskInput struct {
	UserId                    string                     `json:"userId" validate:"required"`
	SasToken                  string                     `json:"sasToken" validate:"required"`
	BlobStoreBaseUri          string                     `json:"blobStoreBaseUri" validate:"required"`
	RealizationSurfaceObjects []realizationSurfaceObject `json:"realizationSurfaceObjects" validate:"required"`
	PointSets                 []pointSet                 `json:"pointSets" validate:"required"`
}

type realizationSampleResult struct {
	Realization   int       `msgpack:"realization"   json:"realization"`
	SampledValues []float32 `msgpack:"sampledValues" json:"sampledValues"`
}

type sampleInPointsTaskResult struct {
	SampleResultArr []realizationSampleResult `msgpack:"sampleResultArr"  json:"sampleResultArr"`
	UndefLimit      float32                   `msgpack:"undefLimit"       json:"undefLimit"`
}

func (deps *TaskDeps) ProcessSampleInPointsTask(ctx context.Context, task *asynq.Task) error {
	perfMetrics := utils.NewPerfMetrics()
	logger := slog.Default()
	prefix := "ProcessSampleInPointsTask() - "

	logger.Debug(prefix + "entering")

	var input sampleInPointsTaskInput
	if err := json.Unmarshal(task.Payload(), &input); err != nil {
		logger.Error(prefix+"failed to unmarshal task input", "err", err)
		return err
	}

	validate := validator.New()
	if err := validate.Struct(input); err != nil {
		logger.Error(prefix+"validation error:", "err", err)
		return err
	}

	/*
		perRealObjIds := make([]operations.RealObjId, len(input.ObjectIds))
		for i := range input.ObjectIds {
			perRealObjIds[i] = operations.RealObjId(input.ObjectIds[i])
		}

		blobFetcher := utils.NewBlobFetcher(input.SasToken, input.BlobStoreBaseUri)

		perfMetrics.RecordLap("init")

		perRealSamplesArr, err := operations.BulkFetchAndSampleSurfaces(blobFetcher, perRealObjIds, input.XCoords, input.YCoords)
		if err != nil {
			logger.Error(prefix+"error during bulk processing of surfaces:", "err", err)
			return err
		}
		perfMetrics.RecordLap("fetch-and-sample")

		retResultArr := make([]realizationSampleResult, len(perRealSamplesArr))
		for i := range retResultArr {
			retResultArr[i] = realizationSampleResult(perRealSamplesArr[i])
		}
	*/

	perRealObjIds := make([]operations.RealSurfObj, len(input.RealizationSurfaceObjects))
	for i := range input.RealizationSurfaceObjects {
		perRealObjIds[i] = operations.RealSurfObj(input.RealizationSurfaceObjects[i])
	}

	blobFetcher := utils.NewBlobFetcher(input.SasToken, input.BlobStoreBaseUri)

	numPointSets := len(input.PointSets)
	pointSetArr := make([]operations.PointSet, numPointSets)
	for i := range input.PointSets {
		pointSetArr[i] = operations.PointSet{
			Name:    input.PointSets[i].Name,
			XCoords: input.PointSets[i].XCoords,
			YCoords: input.PointSets[i].YCoords,
		}
	}

	perfMetrics.RecordLap("init")

	pointSetResultArr, err := operations.FetchAndSampleSurfacesInPointSets(blobFetcher, perRealObjIds, pointSetArr)
	if err != nil {
		logger.Error(prefix+"error during bulk processing of surfaces:", "err", err)
		return err
	}
	perfMetrics.RecordLap("fetch-and-sample")

	singlePointSetResult := pointSetResultArr[0]
	logger.Debug(prefix+"fetched and sampled surfaces", "numSamples", len(singlePointSetResult.PerRealSamples))
	retResultArr := make([]realizationSampleResult, len(singlePointSetResult.PerRealSamples))
	for i := range retResultArr {
		retResultArr[i] = realizationSampleResult(singlePointSetResult.PerRealSamples[i])
	}

	taskResult := sampleInPointsTaskResult{
		SampleResultArr: retResultArr,
		UndefLimit:      0.99e30,
	}

	tempUserStore := deps.tempUserStoreFactory.ForUser(input.UserId)

	// blobExtension := "json"
	// bytePayload, err := json.Marshal(taskResult)
	// if err != nil {
	// 	logger.Error(prefix+"failed to encode task result as json", "err", err)
	// 	return err
	// }

	blobExtension := "msgpack"
	bytePayload, err := msgpack.Marshal(taskResult)
	if err != nil {
		logger.Error(prefix+"failed to encode task result as msgpack", "err", err)
		return err
	}

	err = tempUserStore.PutBytes(ctx, input.PointSets[0].TargetStoreKey, bytePayload, "sampleInPoints", blobExtension)
	if err != nil {
		logger.Error(prefix+"failed to write result to temp user store", "err", err)
		return err
	}

	perfMetrics.RecordLap("store")

	payloadSizeMB := float32(len(bytePayload)) / (1024 * 1024)
	logger.Info(prefix + fmt.Sprintf("task completed in %s (resultPayload=%.2fMB)", perfMetrics.ToString(true), payloadSizeMB))

	return nil
}
