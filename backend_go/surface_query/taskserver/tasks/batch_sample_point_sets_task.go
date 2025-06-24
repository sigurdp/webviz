package tasks

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"surface_query/taskserver/logic/batch_sample_point_sets"
	"surface_query/utils"

	"github.com/go-playground/validator/v10"
	"github.com/hibiken/asynq"
	"github.com/vmihailenco/msgpack/v5"
)

type NamedPointSet struct {
	Name           string    `json:"name" validate:"required"`
	XCoords        []float64 `json:"xCoords" validate:"required"`
	YCoords        []float64 `json:"yCoords" validate:"required"`
	TargetStoreKey string    `json:"targetStoreKey" validate:"required"`
}

type BatchSamplePointSetsTaskInput struct {
	UserId                    string                     `json:"userId" validate:"required"`
	SasToken                  string                     `json:"sasToken" validate:"required"`
	BlobStoreBaseUri          string                     `json:"blobStoreBaseUri" validate:"required"`
	RealizationSurfaceObjects []RealizationSurfaceObject `json:"realizationSurfaceObjects" validate:"required"`
	PointSets                 []NamedPointSet            `json:"pointSets" validate:"required"`
}

func (deps *TaskDeps) ProcessBatchSamplePointSetsTask(ctx context.Context, task *asynq.Task) error {
	perfMetrics := utils.NewPerfMetrics()
	logger := slog.Default()
	prefix := "ProcessBatchSamplePointSetsTask() - "

	logger.Debug(prefix + "entering")

	var input BatchSamplePointSetsTaskInput
	if err := json.Unmarshal(task.Payload(), &input); err != nil {
		logger.Error(prefix+"failed to unmarshal task input", "err", err)
		return err
	}

	validate := validator.New()
	if err := validate.Struct(input); err != nil {
		logger.Error(prefix+"validation error:", "err", err)
		return err
	}

	perRealObjIds := make([]batch_sample_point_sets.RealSurfObj, len(input.RealizationSurfaceObjects))
	for i := range input.RealizationSurfaceObjects {
		perRealObjIds[i] = batch_sample_point_sets.RealSurfObj(input.RealizationSurfaceObjects[i])
	}

	blobFetcher := utils.NewBlobFetcher(input.SasToken, input.BlobStoreBaseUri)

	numPointSets := len(input.PointSets)
	pointSetArr := make([]batch_sample_point_sets.PointSet, numPointSets)
	for ips := range input.PointSets {
		pointSetArr[ips] = batch_sample_point_sets.PointSet{
			Name:    input.PointSets[ips].Name,
			XCoords: input.PointSets[ips].XCoords,
			YCoords: input.PointSets[ips].YCoords,
		}
	}

	perfMetrics.RecordLap("init")

	logger.Debug(prefix+"starting fetch and sample", "numRealizations", len(perRealObjIds), "numPointSets", numPointSets)

	perPointSetResults, err := batch_sample_point_sets.FetchAndBatchSampleInPointSets(blobFetcher, perRealObjIds, pointSetArr)
	if err != nil {
		logger.Error(prefix+"error during bulk processing of surfaces:", "err", err)
		return err
	}
	perfMetrics.RecordLap("fetch-and-sample")

	// free memory for point set array
	pointSetArr = nil

	numPointSetsWithResults := len(perPointSetResults)
	logger.Debug(prefix+"fetch and sample done", "numPointSetResults", numPointSetsWithResults)

	tempUserStore := deps.tempUserStoreFactory.ForUser(input.UserId)

	accumulatedPayloadMB := float32(0)

	for ips := range input.PointSets {
		pointSetName := input.PointSets[ips].Name
		pointSetTargetStoreKey := input.PointSets[ips].TargetStoreKey

		resultsForThisPointSet := perPointSetResults[ips]
		perPointSetResults[ips] = nil // free memory for this point set result
		numRealizationsSampled := len(resultsForThisPointSet.PerRealSamples)

		taskResult := SampleInPointsTaskResult{
			RealizationSamples: make([]RealizationValues, numRealizationsSampled),
			UndefLimit:         0.99e30,
		}

		for ir := 0; ir < numRealizationsSampled; ir++ {
			taskResult.RealizationSamples[ir] = RealizationValues{
				Realization:   resultsForThisPointSet.PerRealSamples[ir].Realization,
				SampledValues: resultsForThisPointSet.PerRealSamples[ir].SampledValues,
			}
		}

		blobExtension := "msgpack"
		bytePayload, err := msgpack.Marshal(taskResult)
		if err != nil {
			logger.Error(prefix+"failed to encode task result as msgpack", "err", err)
			return err
		}

		err = tempUserStore.PutBytes(ctx, pointSetTargetStoreKey, bytePayload, "sampleInPoints", blobExtension)
		if err != nil {
			logger.Error(prefix+"failed to write result to temp user store", "err", err)
			return err
		}

		payloadSizeMB := float32(len(bytePayload)) / (1024 * 1024)
		accumulatedPayloadMB += payloadSizeMB
		logger.Debug(prefix + fmt.Sprintf("result payload for point set '%s' stored (resultPayload=%.2fMB)", pointSetName, payloadSizeMB))
	}

	perfMetrics.RecordLap("store")

	logger.Info(prefix + fmt.Sprintf("task completed in %s (numPointSets=%d, accumulatedPayloadMB=%.2fMB)", perfMetrics.ToString(true), numPointSets, accumulatedPayloadMB))

	return nil
}
