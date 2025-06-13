package httpbridge

import (
	"github.com/kelseyhightower/envconfig"
)

type Config struct {
	RedisUrl string `envconfig:"REDIS_URL" default:"redis://redis-cache:6379"`
}

func LoadConfig(cfg *Config) error {
	err := envconfig.Process("", cfg)
	//slog.Debug("Config:", "cfg", cfg)
	return err
}
