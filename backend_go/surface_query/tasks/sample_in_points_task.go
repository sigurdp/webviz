package tasks

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"surface_query/operations"
	"surface_query/utils"
	"time"

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
	Realization   int       `json:"realization" binding:"required"`
	SampledValues []float32 `json:"sampledValues" binding:"required"`
}

type sampleInPointsTaskResult struct {
	SampleResultArr []realizationSampleResult `json:"sampleResultArr" binding:"required"`
	UndefLimit      float32                   `json:"undefLimit" binding:"required"`
}

func (deps *TaskDeps) ProcessSampleInPointsTask(ctx context.Context, task *asynq.Task) error {
	logger := slog.Default()
	prefix := "ProcessSampleInPointsTask - "

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

	startTime := time.Now()

	perRealObjIds := make([]operations.RealObjId, len(input.ObjectIds))
	for i := range input.ObjectIds {
		perRealObjIds[i] = operations.RealObjId(input.ObjectIds[i])
	}

	blobFetcher := utils.NewBlobFetcher(input.SasToken, input.BlobStoreBaseUri)
	perRealSamplesArr, err := operations.BulkFetchAndSampleSurfaces(blobFetcher, perRealObjIds, input.XCoords, input.YCoords)
	if err != nil {
		logger.Error(prefix+"error during bulk processing of surfaces:", "err", err)
		return err
	}

	// Construct the response body
	//
	// TO-DISCUSS:
	// Must check this out in relation to the xtgeo code
	// Undef value and limit seem to be misaligned!!!
	retResultArr := make([]realizationSampleResult, len(perRealSamplesArr))
	for i := range retResultArr {
		retResultArr[i] = realizationSampleResult(perRealSamplesArr[i])
	}

	taskResult := sampleInPointsTaskResult{
		SampleResultArr: retResultArr,
		UndefLimit:      0.99e30,
	}

	resultPayload, err := json.Marshal(taskResult)
	if err != nil {
		logger.Error(prefix+"failed to marshal task result", "err", err)
		return err
	}

	tempUserStore := deps.tempUserStoreFactory.ForUser(input.SpikeUserId)
	err = tempUserStore.PutBytes(ctx, input.SpikeResultCacheKey, resultPayload, "sampleInPoints", "json")
	if err != nil {
		logger.Error(prefix+"failed to put bytes in temp user store", "err", err)
		return err
	}

	logger.Debug(prefix+"bytes written", "n", len(resultPayload))

	duration := time.Since(startTime)
	logger.Info(prefix + fmt.Sprintf("Total time: %v", duration))

	return nil
}
