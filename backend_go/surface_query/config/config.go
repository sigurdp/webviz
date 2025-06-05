package config

import (
	"github.com/kelseyhightower/envconfig"
)

type Config struct {
	RedisUrl                     string `envconfig:"REDIS_URL" default:"redis://redis-cache:6379"`
	AzureStorageConnectionString string `envconfig:"AZURE_STORAGE_CONNECTION_STRING" required:"true"`
	AzureStorageContainerName    string `envconfig:"AZURE_STORAGE_CONTAINER_NAME" default:"test-user-scoped-temp-storage"`
}

func Load(cfg *Config) error {
	err := envconfig.Process("", cfg)
	//slog.Debug("Config:", "cfg", cfg)
	return err
}
