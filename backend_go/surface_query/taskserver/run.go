package taskserver

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"surface_query/taskserver/tasks"
	"surface_query/utils"

	"github.com/hibiken/asynq"
)

func Run() {
	logger := slog.Default()

	logger.Info("Starting surface query asynq task server...")

	var cfg Config
	if err := LoadConfig(&cfg); err != nil {
		logger.Error("failed to load config from environment variables:", "err", err)
		os.Exit(1)
	}
	logger.Info(fmt.Sprintf("RedisUrl=%v", cfg.RedisUrl))

	tempUserStoreFactory := utils.NewTempUserStoreFactory(cfg.RedisUrl, cfg.AzureStorageConnectionString, 60*60)

	asynqRedisConnOpt, err := asynq.ParseRedisURI(cfg.RedisUrl)
	if err != nil {
		logger.Error("failed to parse Redis URL for usage with asynq", "err", err)
		os.Exit(1)
	}

	// A good starting point for asynq concurrency is double the number of logical cores
	// goMaxProcs := runtime.GOMAXPROCS(0)
	// wantedAsynqConcurrency := goMaxProcs * 2
	// Then again that can easily blow up the memory consumption, so limit ourselves as an experiment for now
	// Try with a hard limit of 2, may even want just a single task!!
	wantedAsynqConcurrency := 1
	asynqServer := asynq.NewServer(
		asynqRedisConnOpt,
		asynq.Config{
			Concurrency:       wantedAsynqConcurrency,
			TaskCheckInterval: 500 * time.Millisecond,
		},
	)

	taskDeps := tasks.NewTaskDeps(tempUserStoreFactory)

	taskMux := asynq.NewServeMux()
	taskMux.HandleFunc(tasks.TaskType_Dummy, taskDeps.ProcessDummyTask)
	taskMux.HandleFunc(tasks.TaskType_SampleInPoints, taskDeps.ProcessSampleInPointsTask)
	taskMux.HandleFunc(tasks.TaskType_BatchSamplePointSets, taskDeps.ProcessBatchSamplePointSetsTask)

	// Start Asynq server in goroutine
	go func() {
		logger.Info(fmt.Sprintf("Starting Asynq server with concurrency=%d ...", wantedAsynqConcurrency))
		if err := asynqServer.Run(taskMux); err != nil {
			logger.Error("asynq server error", "err", err)
			os.Exit(1)
		}
	}()

	// Signal handling for graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.Info("Shutting down Asynq server...")
	asynqServer.Shutdown()
}
