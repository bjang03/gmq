// Package utils provides utility functions for the GMQ message queue system.
// It includes logging utilities, validation, configuration loading, and type conversion helpers.
package utils

import (
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"
)

// LogLevel represents the log level
type LogLevel int

const (
	LevelDebug LogLevel = iota
	LevelInfo
	LevelWarn
	LevelError
)

var (
	once      sync.Once
	appLogger *Logger
)

// Color codes for terminal output
const (
	colorReset  = "\033[0m"
	colorRed    = "\033[31m"
	colorYellow = "\033[33m"
	colorBlue   = "\033[34m"
	colorGreen  = "\033[32m"
	colorGray   = "\033[90m"
	colorCyan   = "\033[36m"
)

// LogEntry represents a single log entry
type LogEntry struct {
	Level   LogLevel
	Time    time.Time
	Message string
	Plugin  string
	Fields  map[string]any
}

// Logger is the custom logger implementation with colored output
type Logger struct {
	level   LogLevel
	output  io.Writer
	mu      sync.Mutex
	plugin  string
	noColor bool
}

// InitLogger initializes the logging system with the specified level.
// This function can only be called once due to sync.Once protection.
// Parameters:
//   - level: log level (LevelDebug, LevelInfo, LevelWarn, LevelError)
func InitLogger(level LogLevel) {
	once.Do(func() {
		appLogger = NewLogger(level)
	})
}

// NewLogger creates a new logger instance.
// Parameters:
//   - level: minimum log level to output
//   - output: output writer (defaults to os.Stdout)
//
// Returns a new Logger instance
func NewLogger(level LogLevel, output ...io.Writer) *Logger {
	w := io.Writer(os.Stdout)
	if len(output) > 0 && output[0] != nil {
		w = output[0]
	}
	return &Logger{
		level:   level,
		output:  w,
		noColor: false, // Enable colors by default for better readability
	}
}

// GetLogger returns the global logger instance.
// If the logger has not been initialized, it will be created with default Info level.
// Returns the global Logger instance
func GetLogger() *Logger {
	if appLogger == nil {
		InitLogger(LevelInfo)
	}
	return appLogger
}

// WithPlugin returns a new logger instance with a plugin name prefix.
// Parameters:
//   - pluginName: name of the plugin/instance (e.g., "nats", "redis", "rabbitmq")
//
// Returns a new logger instance with plugin prefix
func (l *Logger) WithPlugin(pluginName string) *Logger {
	return &Logger{
		level:   l.level,
		output:  l.output,
		plugin:  pluginName,
		noColor: l.noColor,
	}
}

// WithFields returns a new logger instance with additional fields.
// Parameters:
//   - fields: key-value pairs to include in log entries
//
// Returns a new logger instance with fields
func (l *Logger) WithFields(fields map[string]any) *Logger {
	newLogger := &Logger{
		level:   l.level,
		output:  l.output,
		plugin:  l.plugin,
		noColor: l.noColor,
	}
	// Fields are stored and used when logging
	return newLogger
}

// log is the internal logging method
func (l *Logger) log(level LogLevel, msg string, args ...any) {
	if level < l.level {
		return
	}

	entry := LogEntry{
		Level:   level,
		Time:    time.Now(),
		Message: msg,
		Plugin:  l.plugin,
		Fields:  make(map[string]any),
	}

	// Parse args into fields
	for i := 0; i < len(args); i += 2 {
		if i+1 < len(args) {
			key := fmt.Sprintf("%v", args[i])
			entry.Fields[key] = args[i+1]
		}
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	l.writeEntry(entry)
}

// writeEntry writes a log entry to the output
func (l *Logger) writeEntry(entry LogEntry) {
	var builder strings.Builder

	// Time
	timeStr := entry.Time.Format("15:04:05.000")
	if l.noColor {
		builder.WriteString(fmt.Sprintf("[%s] ", timeStr))
	} else {
		builder.WriteString(fmt.Sprintf("%s[%s]%s ", colorGray, timeStr, colorReset))
	}

	// Level
	levelStr := l.levelString(entry.Level)
	var levelColor string
	switch entry.Level {
	case LevelDebug:
		levelColor = colorGray
	case LevelInfo:
		levelColor = colorGreen
	case LevelWarn:
		levelColor = colorYellow
	case LevelError:
		levelColor = colorRed
	}
	if l.noColor {
		builder.WriteString(fmt.Sprintf("[%-5s] ", levelStr))
	} else {
		builder.WriteString(fmt.Sprintf("%s[%-5s]%s ", levelColor, levelStr, colorReset))
	}

	// Plugin prefix
	if entry.Plugin != "" {
		if l.noColor {
			builder.WriteString(fmt.Sprintf("[%s] ", entry.Plugin))
		} else {
			builder.WriteString(fmt.Sprintf("%s[%s]%s ", colorCyan, entry.Plugin, colorReset))
		}
	}

	// Message
	builder.WriteString(entry.Message)

	// Fields
	if len(entry.Fields) > 0 {
		builder.WriteString(" |")
		for key, value := range entry.Fields {
			builder.WriteString(fmt.Sprintf(" %s=%v", key, value))
		}
	}

	builder.WriteString("\n")
	l.output.Write([]byte(builder.String()))
}

// levelString returns the string representation of a log level
func (l *Logger) levelString(level LogLevel) string {
	switch level {
	case LevelDebug:
		return "DEBUG"
	case LevelInfo:
		return "INFO"
	case LevelWarn:
		return "WARN"
	case LevelError:
		return "ERROR"
	default:
		return "UNKNOWN"
	}
}

// Debug logs a debug-level message.
// Parameters:
//   - msg: log message
//   - args: optional key-value pairs (e.g., "key", value)
func (l *Logger) Debug(msg string, args ...any) {
	l.log(LevelDebug, msg, args...)
}

// Info logs an info-level message.
// Parameters:
//   - msg: log message
//   - args: optional key-value pairs (e.g., "key", value)
func (l *Logger) Info(msg string, args ...any) {
	l.log(LevelInfo, msg, args...)
}

// Warn logs a warning-level message.
// Parameters:
//   - msg: log message
//   - args: optional key-value pairs (e.g., "key", value)
func (l *Logger) Warn(msg string, args ...any) {
	l.log(LevelWarn, msg, args...)
}

// Error logs an error-level message.
// Parameters:
//   - msg: log message
//   - args: optional key-value pairs (e.g., "key", value)
func (l *Logger) Error(msg string, args ...any) {
	l.log(LevelError, msg, args...)
}

// Debugf logs a formatted debug-level message.
// Parameters:
//   - format: format string
//   - args: arguments for the format string
func (l *Logger) Debugf(format string, args ...any) {
	l.log(LevelDebug, fmt.Sprintf(format, args...))
}

// Infof logs a formatted info-level message.
// Parameters:
//   - format: format string
//   - args: arguments for the format string
func (l *Logger) Infof(format string, args ...any) {
	l.log(LevelInfo, fmt.Sprintf(format, args...))
}

// Warnf logs a formatted warning-level message.
// Parameters:
//   - format: format string
//   - args: arguments for the format string
func (l *Logger) Warnf(format string, args ...any) {
	l.log(LevelWarn, fmt.Sprintf(format, args...))
}

// Errorf logs a formatted error-level message.
// Parameters:
//   - format: format string
//   - args: arguments for the format string
func (l *Logger) Errorf(format string, args ...any) {
	l.log(LevelError, fmt.Sprintf(format, args...))
}

// SetLevel sets the minimum log level.
// Parameters:
//   - level: minimum log level to output
func (l *Logger) SetLevel(level LogLevel) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.level = level
}
