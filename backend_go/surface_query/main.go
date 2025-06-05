package main

import (
	"fmt"
	"log"
	"log/slog"
	"os"
	"runtime"
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
	logger.Info(fmt.Sprintf("AzureStorageContainerName=%v", cfg.AzureStorageContainerName))

	// Can be used to force the number of CPUs that can be executing simultaneously
	// Should not be needed as long as automaxprocs does its job
	//runtime.GOMAXPROCS(4)

	numCpus := runtime.NumCPU()
	goMaxProcs := runtime.GOMAXPROCS(0)
	logger.Info(fmt.Sprintf("Num logical CPUs=%v, GOMAXPROCS=%v", numCpus, goMaxProcs))

	router := gin.New()
	router.Use(utils.SlogBackedGinLogger(logger))
	router.Use(gin.Recovery())

	// ============================================================================================

	tempUserStoreFactory := utils.NewTempUserStoreFactory(cfg.RedisUrl, cfg.AzureStorageConnectionString, cfg.AzureStorageContainerName, 2*time.Minute)

	// Parse the RedisUrl for usage with asynq
	asynqRedisConnOpt, err := asynq.ParseRedisURI(cfg.RedisUrl)
	if err != nil {
		logger.Error("failed to parse Redis URL for usage with asynq", "err", err)
		os.Exit(1)
	}

	asynqServer := asynq.NewServer(
		asynqRedisConnOpt,
		asynq.Config{Concurrency: 2},
	)

	taskDeps := tasks.NewTaskDeps(tempUserStoreFactory)

	mux := asynq.NewServeMux()
	mux.HandleFunc("dummy", taskDeps.ProcessDummyTask)
	mux.HandleFunc("sample_in_points", taskDeps.ProcessSampleInPointsTask)

	go func() {
		if err := asynqServer.Run(mux); err != nil {
			log.Fatalf("could not start Asynq server: %v", err)
		}
	}()
	logger.Info("Asynq Worker is running...")

	httpBridgeHandler := tasks.NewHttpBridgeHandlers(asynqRedisConnOpt)
	httpBridgeHandler.MapRoutes(router)

	// ============================================================================================

	router.GET("/", handlers.HandleRoot)
	router.POST("/sample_in_points", handlers.HandleSampleInPoints)

	address := "0.0.0.0:5001"
	logger.Info(fmt.Sprintf("Surface query server listening on: %v", address))
	router.Run(address)
}
