package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"log/slog"

	"github.com/gin-gonic/gin"
	"github.com/hibiken/asynq"
)

type dummyOpRequest struct {
	MyString string `json:"myString" binding:"required"`
	MyInt    int    `json:"myInt" binding:"required"`
}

type dummyOpResult struct {
	TheValue string `json:"theValue" binding:"required"`
}

type dummyOpStatusResponse struct {
	TaskID string         `json:"task_id" binding:"required"`
	Status string         `json:"status" binding:"required"` // "pending", "inProgress", "completed"
	Result *dummyOpResult `json:"result,omitempty"`
	Error  string         `json:"error,omitempty"`
}

type DummyOpHandlers struct {
	asynqClient    *asynq.Client
	asyncInspector *asynq.Inspector
}

func NewDummyOpHandlers(redisOpt asynq.RedisClientOpt) *DummyOpHandlers {
	client := asynq.NewClient(redisOpt)
	inspector := asynq.NewInspector(redisOpt)
	handlers := DummyOpHandlers{asynqClient: client, asyncInspector: inspector}
	return &handlers
}

func (doh DummyOpHandlers) HandleEnqueueDummyOp(c *gin.Context) {
	slog.Debug("entering HandleEnqueueDummyOp()")

	var reqBody dummyOpRequest
	if err := c.ShouldBindJSON(&reqBody); err != nil {
		slog.Error("Error parsing request body:", "err", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	payloadBytes, err := json.Marshal(reqBody)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "failed to encode payload"})
		return
	}

	task := asynq.NewTask("test:dummyOp", payloadBytes)

	taskInfo, err := doh.asynqClient.Enqueue(task, asynq.MaxRetry(0), asynq.Retention(time.Hour*24))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to enqueue task"})
		return
	}

	c.JSON(http.StatusOK, dummyOpStatusResponse{TaskID: taskInfo.ID, Status: taskInfo.State.String()})
}

func (doh DummyOpHandlers) HandleStatusDummyOp(c *gin.Context) {
	slog.Debug("entering HandleStatusDummyOp()")

	taskID := c.Param("task_id")
	//slog.Debug("HandleStatusDummyOp() - ", "taskID", taskID)

	taskInfo, err := doh.asyncInspector.GetTaskInfo("default", taskID)
	if err == nil {
		slog.Debug("HandleStatusDummyOp() - task info:", "taskID", taskID, "lastErr", taskInfo.LastErr)

		var result *dummyOpResult
		if taskInfo.Result != nil {
			if err := json.Unmarshal(taskInfo.Result, &result); err != nil {
				slog.Error("Error unmarshalling task result:", "err", err)
				c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to decode task result"})
				return
			}
		}

		response := dummyOpStatusResponse{
			TaskID: taskID,
			Status: taskInfo.State.String(),
			Result: result,
			Error:  taskInfo.LastErr,
		}

		c.JSON(http.StatusOK, response)
		return
	}

	slog.Error("Error geting task info:", "err", err)
	c.JSON(http.StatusNotFound, gin.H{"error": "task not found"})
}

func HandleDummyOpTask(ctx context.Context, task *asynq.Task) error {
	slog.Debug("entering HandleDummyOpTask()")

	var reqBody dummyOpRequest
	if err := json.Unmarshal(task.Payload(), &reqBody); err != nil {
		return err
	}

	time.Sleep(2 * time.Second)
	combinedString := reqBody.MyString + " " + strconv.Itoa(reqBody.MyInt)

	result := dummyOpResult{
		TheValue: combinedString,
	}
	slog.Debug("Dummy operation result:", "result", result)

	resultPayload, err := json.Marshal(result)
	if err != nil {
		return err
	}

	n, err := task.ResultWriter().Write(resultPayload)
	if err != nil {
		return fmt.Errorf("failed to write task result: %v", err)
	}

	slog.Debug("bytes written", "n", n)

	return nil
}
