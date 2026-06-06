// Package logger provides a structured JSON logger built on logrus.
// One global logger per process, hot-reloadable log level.
//
// Usage:
//
//	log := logger.New()
//	log.WithFields(logger.Fields{"error": err.Error(), "source": "Init"}).Error("startup failed")
package logger

import (
	"os"
	"strings"

	log "github.com/sirupsen/logrus"
)

// Fields is a type alias for logrus.Fields so callers don't need to import logrus directly.
type Fields = log.Fields

// Logger wraps *logrus.Logger and exposes the full logrus API.
type Logger struct {
	*log.Logger
}

var global = log.New()

// New returns a Logger writing JSON to stdout.
// Level is read from the LOG_LEVEL env var at startup; call SetLogLevel to change at runtime.
func New() *Logger {
	global.SetFormatter(&log.JSONFormatter{})
	global.SetOutput(os.Stdout)
	global.SetLevel(parseLevel(os.Getenv("LOG_LEVEL")))
	return &Logger{Logger: global}
}

// SetLogLevel updates the runtime level and returns the applied value.
// Unrecognised inputs default to INFO.
func SetLogLevel(name string) log.Level {
	lvl := parseLevel(name)
	global.SetLevel(lvl)
	return lvl
}

// CurrentLogLevel returns the active level (for health/admin endpoints).
func CurrentLogLevel() log.Level {
	return global.GetLevel()
}

func parseLevel(s string) log.Level {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return log.DebugLevel
	case "warn", "warning":
		return log.WarnLevel
	case "error":
		return log.ErrorLevel
	case "fatal":
		return log.FatalLevel
	default:
		return log.InfoLevel
	}
}
