package httpbridge

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"surface_query/utils"

	"github.com/gin-gonic/gin"
	"github.com/hibiken/asynq"
)

func Run() {
	logger := slog.Default()

	logger.Info("Starting surface query http bridge...")

	var cfg Config
	if err := LoadConfig(&cfg); err != nil {
		logger.Error("failed to load config from environment variables:", "err", err)
		os.Exit(1)
	}
	logger.Info(fmt.Sprintf("RedisUrl=%v", cfg.RedisUrl))

	// Parse the RedisUrl for usage with asynq
	asynqRedisConnOpt, err := asynq.ParseRedisURI(cfg.RedisUrl)
	if err != nil {
		logger.Error("failed to parse Redis URL for usage with asynq", "err", err)
		os.Exit(1)
	}

	// Set up Gin router
	router := gin.New()
	router.Use(utils.SlogBackedGinLogger(logger))
	router.Use(gin.Recovery())

	bridgeHandlers := NewBridgeHandlers(asynqRedisConnOpt)
	bridgeHandlers.MapRoutes(router)

	// HTTP Server
	address := "0.0.0.0:5001"
	httpServer := &http.Server{
		Addr:    address,
		Handler: router,
	}

	// Start Gin HTTP server in goroutine
	go func() {
		logger.Info(fmt.Sprintf("Starting HTTP server on: %v ...", address))

		err := httpServer.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("HTTP server error", "err", err)
			os.Exit(1)
		}
	}()

	// Signal handling for graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	// Start shutdown sequence
	shutdownTimeout := 10 * time.Second
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer shutdownCancel()

	logger.Info("Stopping HTTP server...")
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		logger.Error("Server shutdown error", "err", err)
		os.Exit(1)
	}

	logger.Info("Shutdown complete")
}
