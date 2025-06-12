package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	_ "go.uber.org/automaxprocs"

	_ "github.com/joho/godotenv/autoload"

	"surface_query/config"
	"surface_query/handlers"
	"surface_query/tasks"
	"surface_query/utils"

	"github.com/gin-gonic/gin"
	"github.com/hibiken/asynq"
	"github.com/lmittmann/tint"
)

func main() {
	logger := slog.New(tint.NewHandler(os.Stdout, &tint.Options{Level: slog.LevelDebug, TimeFormat: time.TimeOnly}))
	slog.SetDefault(logger)

	logger.Info("Starting surface query server...")

	var cfg config.Config
	if err := config.Load(&cfg); err != nil {
		logger.Error("failed to load config from environment variables:", "err", err)
		os.Exit(1)
	}
	logger.Info(fmt.Sprintf("RedisUrl=%v", cfg.RedisUrl))

	// Can be used to force the number of CPUs that can be executing simultaneously
	// Should not be needed as long as automaxprocs does its job
	//runtime.GOMAXPROCS(2)

	numCpus := runtime.NumCPU()
	goMaxProcs := runtime.GOMAXPROCS(0)
	logger.Info(fmt.Sprintf("Num logical CPUs=%v, GOMAXPROCS=%v", numCpus, goMaxProcs))

	// Set up Asynq server
	// ------------------------------------------------
	tempUserStoreFactory := utils.NewTempUserStoreFactory(cfg.RedisUrl, cfg.AzureStorageConnectionString, 2*60)

	// Parse the RedisUrl for usage with asynq
	asynqRedisConnOpt, err := asynq.ParseRedisURI(cfg.RedisUrl)
	if err != nil {
		logger.Error("failed to parse Redis URL for usage with asynq", "err", err)
		os.Exit(1)
	}

	// A good starting point for asynq concurrency is double the number of logical cores
	//wantedAsynqConcurrency := goMaxProcs * 2
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
	taskMux.HandleFunc("dummy", taskDeps.ProcessDummyTask)
	taskMux.HandleFunc("sample_in_points", taskDeps.ProcessSampleInPointsTask)

	// Set up Gin router
	// ------------------------------------------------
	router := gin.New()
	router.Use(utils.SlogBackedGinLogger(logger))
	router.Use(gin.Recovery())

	httpBridgeHandler := tasks.NewHttpBridgeHandlers(asynqRedisConnOpt)
	httpBridgeHandler.MapRoutes(router)

	router.GET("/", handlers.HandleRoot)
	router.POST("/sample_in_points", handlers.HandleSampleInPoints)

	// HTTP Server
	address := "0.0.0.0:5001"
	httpServer := &http.Server{
		Addr:    address,
		Handler: router,
	}

	// Error channel to capture server errors
	errChan := make(chan error, 2)

	// Start Asynq server in goroutine
	go func() {
		logger.Info(fmt.Sprintf("Starting Asynq server with concurrency=%d ...", wantedAsynqConcurrency))
		if err := asynqServer.Run(taskMux); err != nil {
			errChan <- err
		}
	}()

	// Start Gin HTTP server in goroutine
	go func() {
		logger.Info(fmt.Sprintf("Starting HTTP server on: %v ...", address))
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errChan <- err
		}
	}()

	// Signal handling (graceful shutdown)
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

	logger.Info(fmt.Sprintf("Surface query server listening on: %v", address))

	// The select will block until one of the channels receives something.
	select {
	case sig := <-signalChan:
		logger.Info(fmt.Sprintf("Received signal: %v. Shutting down...", sig))
	case err := <-errChan:
		logger.Error("Server error, shutting down...", "err", err)
	}

	// Start shutdown sequence
	shutdownTimeout := 10 * time.Second
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer shutdownCancel()

	// Stop Asynq gracefully
	logger.Info("Stopping Asynq server...")
	asynqServer.Shutdown()

	// Stop HTTP server gracefully
	logger.Info("Stopping HTTP server...")
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		logger.Error("HTTP server shutdown error", "err", err)
	}

	logger.Info("Shutdown complete.")
}
