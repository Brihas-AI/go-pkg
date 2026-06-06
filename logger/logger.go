// Package logger provides a structured JSON logger built on logrus.
// One global logger per process, hot-reloadable log level.
//
// Import as a drop-in for the logrus global logger pattern:
//
//	import log "github.com/Brihas-AI/go-pkg/logger"
//
//	log.WithFields(log.Fields{"error": err.Error(), "source": "Init"}).Error("startup failed")
//	log.Infof("server starting on %s", addr)
package logger

import (
	"os"
	"strings"

	log "github.com/sirupsen/logrus"
)

// Fields is a type alias for logrus.Fields — callers need not import logrus directly.
type Fields = log.Fields

// Entry is a type alias for logrus.Entry.
type Entry = log.Entry

// Level is a type alias for logrus.Level.
type Level = log.Level

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

// Setup configures the package-level logger. Call once from main.
func Setup(level string) {
	global.SetFormatter(&log.JSONFormatter{})
	global.SetOutput(os.Stdout)
	global.SetLevel(parseLevel(level))
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

// ── Package-level log functions (drop-in for logrus global usage) ─────────────

func WithField(key string, value any) *log.Entry  { return global.WithField(key, value) }
func WithFields(fields Fields) *log.Entry          { return global.WithFields(fields) }
func WithError(err error) *log.Entry               { return global.WithError(err) }

func Debug(args ...any)                 { global.Debug(args...) }
func Info(args ...any)                  { global.Info(args...) }
func Warn(args ...any)                  { global.Warn(args...) }
func Error(args ...any)                 { global.Error(args...) }
func Fatal(args ...any)                 { global.Fatal(args...) }
func Debugf(f string, args ...any)      { global.Debugf(f, args...) }
func Infof(f string, args ...any)       { global.Infof(f, args...) }
func Warnf(f string, args ...any)       { global.Warnf(f, args...) }
func Errorf(f string, args ...any)      { global.Errorf(f, args...) }
func Fatalf(f string, args ...any)      { global.Fatalf(f, args...) }

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
