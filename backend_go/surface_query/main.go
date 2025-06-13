package main

import (
	"fmt"
	"log/slog"
	"os"
	"runtime"
	"time"

	_ "go.uber.org/automaxprocs"

	_ "github.com/joho/godotenv/autoload"

	"surface_query/httpbridge"
	"surface_query/legacyserver"
	"surface_query/taskserver"

	"github.com/lmittmann/tint"
)

func main() {
	logger := slog.New(tint.NewHandler(os.Stdout, &tint.Options{Level: slog.LevelDebug, TimeFormat: time.TimeOnly}))
	slog.SetDefault(logger)

	serverMode := os.Getenv("SURFACE_QUERY_SERVER_MODE")
	if serverMode == "" {
		serverMode = "legacyserver"
	}

	// Can be used to force the number of CPUs that can be executing simultaneously
	// Should not be needed as long as automaxprocs does its job
	//runtime.GOMAXPROCS(2)

	numCpus := runtime.NumCPU()
	goMaxProcs := runtime.GOMAXPROCS(0)
	logger.Info(fmt.Sprintf("Launching surface query server in mode: %v  (logical CPUs=%v, GOMAXPROCS=%v)", serverMode, numCpus, goMaxProcs))

	switch serverMode {
	case "taskserver":
		taskserver.Run()
		os.Exit(0)
	case "httpbridge":
		httpbridge.Run()
		os.Exit(0)
	case "legacyserver":
		legacyserver.Run()
		os.Exit(0)
	default:
		panic("SURFACE_QUERY_SERVER_MODE must be one of: taskserver, httpbridge, legacyserver")
	}

}
