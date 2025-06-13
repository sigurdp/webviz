package tasks

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/go-playground/validator/v10"
	"github.com/hibiken/asynq"
)

type dummyTaskInput struct {
	MyString    string `json:"myString" validate:"required"`
	MyInt       int    `json:"myInt" validate:"required"`
	DurationSec int    `json:"durationSec" validate:"required"` // Duration in seconds to simulate processing time
	ShouldFail  bool   `json:"shouldFail"`                      // Simulate failure if true
}

type dummyTaskResult struct {
	TheValue string `json:"theValue" binding:"required"`
}

func (deps TaskDeps) ProcessDummyTask(ctx context.Context, task *asynq.Task) error {
	slog.Debug("entering ProcessDummyTask()")

	var input dummyTaskInput
	if err := json.Unmarshal(task.Payload(), &input); err != nil {
		return err
	}

	validate := validator.New()
	if err := validate.Struct(input); err != nil {
		slog.Error("ProcessDummyTask() - validation error:", "error", err)
		return err
	}

	slog.Debug("ProcessDummyTask() - task input:", "input", input)

	time.Sleep(time.Duration(input.DurationSec) * time.Second)

	if input.ShouldFail {
		slog.Error("ProcessDummyTask() - simulated failure")
		return fmt.Errorf("This is a simulated task failure")
	}

	combinedString := input.MyString + " " + strconv.Itoa(input.MyInt)
	result := dummyTaskResult{
		TheValue: combinedString,
	}
	slog.Debug("ProcessDummyTask() -  operation result:", "result", result)

	resultPayload, err := json.Marshal(result)
	if err != nil {
		return err
	}

	n, err := task.ResultWriter().Write(resultPayload)
	if err != nil {
		return fmt.Errorf("ProcessDummyTask() - failed to write task result: %v", err)
	}

	slog.Debug("ProcessDummyTask() - bytes written", "n", n)

	return nil
}
