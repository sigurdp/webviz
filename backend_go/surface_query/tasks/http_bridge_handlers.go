package tasks

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/hibiken/asynq"
)

type TaskStatusResponse struct {
	TaskId   string      `json:"taskId" binding:"required"`
	Status   string      `json:"status" binding:"required"` // "pending", "running", "succeeded", "failed"
	Result   interface{} `json:"result,omitempty"`          // Optional result field
	ErrorMsg string      `json:"errorMsg,omitempty"`        // If the task failed, this field will contain the error message
}

type HttpBridgeHandlers struct {
	asynqClient    *asynq.Client
	asyncInspector *asynq.Inspector
	queueName      string
}

func NewHttpBridgeHandlers(redisConnOpt asynq.RedisConnOpt) *HttpBridgeHandlers {
	client := asynq.NewClient(redisConnOpt)
	inspector := asynq.NewInspector(redisConnOpt)

	return &HttpBridgeHandlers{
		asynqClient:    client,
		asyncInspector: inspector,
		queueName:      "default",
	}
}

// Sets up the bridge routes
func (h *HttpBridgeHandlers) MapRoutes(router *gin.Engine) {
	router.POST("/enqueue_task/:task_type_name", h.handleEnqueueTask)
	router.GET("/task_status/:task_id", h.handleTaskStatus)
}

func (h *HttpBridgeHandlers) handleEnqueueTask(c *gin.Context) {
	logger := slog.Default()
	prefix := "handleEnqueueTask - "

	taskTypeName := c.Param("task_type_name")

	// We're going to pass the payload directly to the task
	payload, err := c.GetRawData()
	if err != nil {
		logger.Error(prefix+"failed to read request body", "err", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": "failed to read request body"})
		return
	}

	task := asynq.NewTask(taskTypeName, payload)

	taskInfo, err := h.asynqClient.Enqueue(
		task,
		asynq.Queue(h.queueName),
		asynq.MaxRetry(0),
		asynq.Retention(time.Hour*2))

	if err != nil {
		logger.Error(prefix+"failed to enqueue task", "err", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to enqueue task"})
		return
	}

	statusString := mapTaskStateToStatusString(taskInfo.State)
	response := TaskStatusResponse{TaskId: taskInfo.ID, Status: statusString}
	if statusString == "failed" {
		response.ErrorMsg = taskInfo.LastErr
	}

	c.JSON(http.StatusOK, response)
}

func (h *HttpBridgeHandlers) handleTaskStatus(c *gin.Context) {
	logger := slog.Default()
	prefix := "handleTaskStatus - "

	taskId := c.Param("task_id")

	taskInfo, err := h.asyncInspector.GetTaskInfo(h.queueName, taskId)
	if err != nil {
		logger.Error(prefix+"error getting task info:", "err", err)
		c.JSON(http.StatusNotFound, gin.H{"error": "task not found"})
		return
	}

	statusString := mapTaskStateToStatusString(taskInfo.State)

	slog.Debug(prefix+"task info:", "taskId", taskId, "statusString", statusString, "lastErr", taskInfo.LastErr)

	response := TaskStatusResponse{TaskId: taskInfo.ID, Status: statusString}

	if statusString == "failed" {
		response.ErrorMsg = taskInfo.LastErr
	}

	if statusString == "succeeded" && taskInfo.Result != nil {
		var result interface{}
		if err := json.Unmarshal(taskInfo.Result, &result); err != nil {
			logger.Error(prefix+"error unmarshalling task result:", "err", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to decode task result"})
			return
		}

		response.Result = result
	}

	c.JSON(http.StatusOK, response)
}

func mapTaskStateToStatusString(taskState asynq.TaskState) string {
	switch taskState {
	case asynq.TaskStatePending:
		return "pending"
	case asynq.TaskStateActive:
		return "running"
	case asynq.TaskStateCompleted:
		return "succeeded"
	case asynq.TaskStateArchived:
		return "failed"
	}

	panic(fmt.Sprintf("unknown task state: %v", taskState))
}
