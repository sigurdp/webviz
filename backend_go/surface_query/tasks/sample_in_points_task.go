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

type realizationObjectId struct {
	Realization int    `json:"realization" binding:"required"`
	ObjectUuid  string `json:"objectUuid" binding:"required"`
}

type sampleInPointsTaskInput struct {
	SpikeUserId         string                `json:"spike_userId" validate:"required"`
	SpikeResultCacheKey string                `json:"spike_resultCacheKey" validate:"required"`
	SasToken            string                `json:"sasToken" validate:"required"`
	BlobStoreBaseUri    string                `json:"blobStoreBaseUri" validate:"required"`
	ObjectIds           []realizationObjectId `json:"objectIds" validate:"required"`
	XCoords             []float64             `json:"xCoords" validate:"required"`
	YCoords             []float64             `json:"yCoords" validate:"required"`
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

	perRealObjIds := make([]operations.RealSurfObjId, len(input.ObjectIds))
	for i := range input.ObjectIds {
		perRealObjIds[i] = operations.RealSurfObjId(input.ObjectIds[i])
	}

	blobFetcher := utils.NewBlobFetcher(input.SasToken, input.BlobStoreBaseUri)

	perfMetrics.RecordLap("init")
	pointSet := operations.PointSet{
		XCoords: input.XCoords,
		YCoords: input.YCoords,
	}
	pointSetSamples, err := operations.FetchAndSampleSurfacesInPointSets(blobFetcher, perRealObjIds, pointSet)
	if err != nil {
		logger.Error(prefix+"error during bulk processing of surfaces:", "err", err)
		return err
	}
	perfMetrics.RecordLap("fetch-and-sample")

	logger.Debug(prefix+"fetched and sampled surfaces", "numSamples", len(pointSetSamples.RealSampleRes))
	retResultArr := make([]realizationSampleResult, len(pointSetSamples.RealSampleRes))
	for i := range retResultArr {
		retResultArr[i] = realizationSampleResult(pointSetSamples.RealSampleRes[i])
	}

	taskResult := sampleInPointsTaskResult{
		SampleResultArr: retResultArr,
		UndefLimit:      0.99e30,
	}

	tempUserStore := deps.tempUserStoreFactory.ForUser(input.SpikeUserId)

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

	err = tempUserStore.PutBytes(ctx, input.SpikeResultCacheKey, bytePayload, "sampleInPoints", blobExtension)
	if err != nil {
		logger.Error(prefix+"failed to write result to temp user store", "err", err)
		return err
	}

	perfMetrics.RecordLap("store")

	payloadSizeMB := float32(len(bytePayload)) / (1024 * 1024)
	logger.Info(prefix + fmt.Sprintf("task completed in %s (resultPayload=%.2fMB)", perfMetrics.ToString(true), payloadSizeMB))

	return nil
}
