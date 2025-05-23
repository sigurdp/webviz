package main

import (
	"fmt"
	"log"
	"log/slog"
	"os"
	"runtime"
	"time"

	_ "go.uber.org/automaxprocs"

	"surface_query/handlers"

	"github.com/gin-gonic/gin"
	"github.com/hibiken/asynq"
	"github.com/lmittmann/tint"
)

func main() {
	logger := slog.New(tint.NewHandler(os.Stdout, &tint.Options{Level: slog.LevelDebug, TimeFormat: time.TimeOnly}))
	slog.SetDefault(logger)

	logger.Info("Starting surface query server...")

	// Can be used to force the number of CPUs that can be executing simultaneously
	// Should not be needed as long as automaxprocs does its job
	//runtime.GOMAXPROCS(4)

	numCpus := runtime.NumCPU()
	goMaxProcs := runtime.GOMAXPROCS(0)
	logger.Info(fmt.Sprintf("Num logical CPUs=%v, GOMAXPROCS=%v", numCpus, goMaxProcs))

	router := gin.Default()

	// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
	// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
	redisOpt := asynq.RedisClientOpt{
		Addr: "redis-cache:6379",
	}

	srv := asynq.NewServer(
		redisOpt,
		asynq.Config{
			Concurrency: 10,
			Queues: map[string]int{
				"default":  6,
				"critical": 4,
			},
		},
	)

	mux := asynq.NewServeMux()
	mux.HandleFunc("test:dummyOp", handlers.HandleDummyOpTask)

	go func() {
		if err := srv.Run(mux); err != nil {
			log.Fatalf("Could not start Asynq server: %v", err)
		}
	}()

	logger.Info("Asynq Worker is running...")

	dummyOpHandlers := handlers.NewDummyOpHandlers(redisOpt)
	router.POST("/dummy_op", dummyOpHandlers.HandleEnqueueDummyOp)
	router.GET("/dummy_op_status/:task_id", dummyOpHandlers.HandleStatusDummyOp)
	// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
	// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

	router.GET("/", handlers.HandleRoot)
	router.POST("/sample_in_points", handlers.HandleSampleInPoints)

	address := "0.0.0.0:5001"
	logger.Info(fmt.Sprintf("Surface query server listening on: %v", address))
	router.Run(address)
}
