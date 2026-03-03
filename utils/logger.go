// Package utils provides utility functions for the GMQ message queue system.
// It includes logging utilities, validation, configuration loading, and type conversion helpers.
package utils

import (
	"log/slog"
	"os"
	"sync"
)

var (
	once      sync.Once
	appLogger *slog.Logger
)

// InitLogger initializes the logging system with the specified level.
// This function can only be called once due to sync.Once protection.
// Parameters:
//   - level: log level (slog.LevelDebug, slog.LevelInfo, slog.LevelWarn, slog.LevelError)
func InitLogger(level slog.Level) {
	once.Do(func() {
		opts := &slog.HandlerOptions{
			Level: level,
		}
		// default to JSON output, production friendly
		handler := slog.NewJSONHandler(os.Stdout, opts)
		appLogger = slog.New(handler)
		slog.SetDefault(appLogger)
	})
}

// GetLogger returns the global logger instance.
// If the logger has not been initialized, it will be created with default Info level.
// Returns the global slog.Logger instance
func GetLogger() *slog.Logger {
	if appLogger == nil {
		// if not initialized, use default configuration
		InitLogger(slog.LevelInfo)
	}
	return appLogger
}

// LogDebug logs a debug-level message with optional key-value pairs.
// Parameters:
//   - msg: log message
//   - args: optional key-value pairs for structured logging (e.g., "key", value)
func LogDebug(msg string, args ...any) {
	GetLogger().Debug(msg, args...)
}

// LogInfo logs an info-level message with optional key-value pairs.
// Parameters:
//   - msg: log message
//   - args: optional key-value pairs for structured logging (e.g., "key", value)
func LogInfo(msg string, args ...any) {
	GetLogger().Info(msg, args...)
}

// LogWarn logs a warning-level message with optional key-value pairs.
// Parameters:
//   - msg: log message
//   - args: optional key-value pairs for structured logging (e.g., "key", value)
func LogWarn(msg string, args ...any) {
	GetLogger().Warn(msg, args...)
}

// LogError logs an error-level message with optional key-value pairs.
// Parameters:
//   - msg: log message
//   - args: optional key-value pairs for structured logging (e.g., "key", value)
func LogError(msg string, args ...any) {
	GetLogger().Error(msg, args...)
}

// LogWithPlugin returns a logger instance with a plugin name field.
// This allows filtering logs by plugin when multiple message queue instances are running.
// Parameters:
//   - pluginName: name of the plugin/instance (e.g., "nats", "redis", "rabbitmq")
//
// Returns a logger instance with "plugin" field 
// Returns a logger instance with "plugin" field set
func LogWithPlugin(pluginName string) *slog.Logger {
	return GetLogger().With("plugin", pluginName)
}
