package logger

import (
	"os"
	"strings"

	log "github.com/sirupsen/logrus"
)

type Fields = log.Fields
type Entry = log.Entry
type Level = log.Level

type Logger struct {
	*log.Logger
}

var global = log.New()

func New() *Logger {
	global.SetFormatter(&log.JSONFormatter{})
	global.SetOutput(os.Stdout)
	global.SetLevel(parseLevel(os.Getenv("LOG_LEVEL")))
	return &Logger{Logger: global}
}

func Setup(level string) {
	global.SetFormatter(&log.JSONFormatter{})
	global.SetOutput(os.Stdout)
	global.SetLevel(parseLevel(level))
}

func SetLogLevel(name string) log.Level {
	lvl := parseLevel(name)
	global.SetLevel(lvl)
	return lvl
}

func CurrentLogLevel() log.Level {
	return global.GetLevel()
}

func WithField(key string, value any) *log.Entry  { return global.WithField(key, value) }
func WithFields(fields Fields) *log.Entry          { return global.WithFields(fields) }
func WithError(err error) *log.Entry               { return global.WithError(err) }

func Debug(args ...any)                 { global.Debug(args...) }
func Info(args ...any)                  { global.Info(args...) }
func Warn(args ...any)                  { global.Warn(args...) }
func Error(args ...any)                 { global.Error(args...) }
func Fatal(args ...any)                 { global.Fatal(args...) }
func Panic(args ...any)                 { global.Panic(args...) }
func Debugf(f string, args ...any)      { global.Debugf(f, args...) }
func Infof(f string, args ...any)       { global.Infof(f, args...) }
func Warnf(f string, args ...any)       { global.Warnf(f, args...) }
func Errorf(f string, args ...any)      { global.Errorf(f, args...) }
func Fatalf(f string, args ...any)      { global.Fatalf(f, args...) }
func Panicf(f string, args ...any)      { global.Panicf(f, args...) }

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
	case "panic":
		return log.PanicLevel
	default:
		return log.InfoLevel
	}
}
