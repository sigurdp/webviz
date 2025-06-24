package tasks

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"surface_query/taskserver/logic/sample_in_points"
	"surface_query/utils"

	"github.com/go-playground/validator/v10"
	"github.com/hibiken/asynq"
	"github.com/vmihailenco/msgpack/v5"
)

type SampleInPointsTaskInput struct {
	UserId                    string                     `json:"userId" validate:"required"`
	TargetStoreKey            string                     `json:"targetStoreKey" validate:"required"`
	SasToken                  string                     `json:"sasToken" validate:"required"`
	BlobStoreBaseUri          string                     `json:"blobStoreBaseUri" validate:"required"`
	RealizationSurfaceObjects []RealizationSurfaceObject `json:"realizationSurfaceObjects" validate:"required"`
	XCoords                   []float64                  `json:"xCoords" validate:"required"`
	YCoords                   []float64                  `json:"yCoords" validate:"required"`
}

// Task that samples realization surfaces in a set of XY points
//
// Input payload struct: SampleInPointsTaskInput
// Task result struct written to tempUserStore: sampleInPointsTaskResult
func (deps *TaskDeps) ProcessSampleInPointsTask(ctx context.Context, task *asynq.Task) error {
	perfMetrics := utils.NewPerfMetrics()
	logger := slog.Default()
	prefix := "ProcessSampleInPointsTask() - "

	logger.Debug(prefix + "entering")

	var input SampleInPointsTaskInput
	if err := json.Unmarshal(task.Payload(), &input); err != nil {
		logger.Error(prefix+"failed to unmarshal task input", "err", err)
		return err
	}

	validate := validator.New()
	if err := validate.Struct(input); err != nil {
		logger.Error(prefix+"input validation error:", "err", err)
		return err
	}

	numRealizations := len(input.RealizationSurfaceObjects)
	perRealSurfObjs := make([]sample_in_points.RealSurfObj, numRealizations)
	for i := 0; i < numRealizations; i++ {
		perRealSurfObjs[i] = sample_in_points.RealSurfObj(input.RealizationSurfaceObjects[i])
	}

	pointSet := sample_in_points.PointSet{
		XCoords: input.XCoords,
		YCoords: input.YCoords,
	}

	blobFetcher := utils.NewBlobFetcher(input.SasToken, input.BlobStoreBaseUri)

	perfMetrics.RecordLap("init")

	perRealSamples, err := sample_in_points.FetchAndSampleInPoints(blobFetcher, perRealSurfObjs, pointSet)
	if err != nil {
		logger.Error(prefix+"error during fetching and sampling of surfaces:", "err", err)
		return err
	}
	perfMetrics.RecordLap("fetch-and-sample")

	// !!!!!!!!!!!!!!!!!!!!!!!!!!!!
	// !!!!!!!!!!!!!!!!!!!!!!!!!!!!
	// !!!!!!!!!!!!!!!!!!!!!!!!!!!!
	// !!!!!!!!!!!!!!!!!!!!!!!!!!!!
	if len(pointSet.XCoords)%2 != 0 {
		err := fmt.Errorf("INTENTIONAL FAILURE ON ODD NUMBER OF COORDS")
		logger.Error(prefix+"INTENTIONAL FAILURE", "err", err)
		return err
	}

	tempUserStore := deps.tempUserStoreFactory.ForUser(input.UserId)

	taskResult := SampleInPointsTaskResult{
		RealizationSamples: make([]RealizationValues, len(perRealSamples)),
		UndefLimit:         0.99e30,
	}
	for i := range perRealSamples {
		taskResult.RealizationSamples[i] = RealizationValues(perRealSamples[i])
	}

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

	err = tempUserStore.PutBytes(ctx, input.TargetStoreKey, bytePayload, "surfacesSampledInPoints", blobExtension)
	if err != nil {
		logger.Error(prefix+"failed to write result to temp user store", "err", err)
		return err
	}

	perfMetrics.RecordLap("store")

	payloadSizeMB := float32(len(bytePayload)) / (1024 * 1024)
	logger.Info(prefix + fmt.Sprintf("task completed in %s (numRealizations=%d, resultPayloadMB=%.2fMB)", perfMetrics.ToString(true), numRealizations, payloadSizeMB))

	return nil
}
