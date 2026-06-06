// Package env provides lightweight helpers for reading environment variables
// and loading a .env file in local/dev mode.
package env

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/joho/godotenv"
	log "github.com/sirupsen/logrus"
)

// LoadEnv loads a .env file from the working directory.
// No-op in staging/production (ENVIRONMENT must be "local" or "dev").
// Pre-existing env vars are NOT overwritten.
func LoadEnv() {
	env := strings.TrimSpace(os.Getenv("ENVIRONMENT"))
	if env != "" && env != "local" && env != "dev" {
		return
	}

	absPath, _ := filepath.Abs(".env")
	if _, err := os.Stat(absPath); err != nil {
		if os.IsNotExist(err) {
			log.Warnf("env: .env not found at %s — skipping", absPath)
		}
		return
	}

	if err := godotenv.Load(absPath); err != nil {
		log.Errorf("env: failed to load .env: %v", err)
		return
	}

	log.Infof("env: loaded .env from %s", absPath)
}

// GetEnvOrDefault returns the env var value or fallback if unset/empty.
func GetEnvOrDefault(key, fallback string) string {
	if val, ok := os.LookupEnv(key); ok && strings.TrimSpace(val) != "" {
		return val
	}
	return fallback
}

// GetEnvOrDefaultInt returns the env var parsed as int, or fallback.
func GetEnvOrDefaultInt(key string, fallback int) int {
	if val, ok := os.LookupEnv(key); ok {
		if n, err := strconv.Atoi(strings.TrimSpace(val)); err == nil {
			return n
		}
	}
	return fallback
}

// GetEnvOrDefaultBool returns the env var parsed as bool, or fallback.
// Truthy values: "true", "1", "yes".
func GetEnvOrDefaultBool(key string, fallback bool) bool {
	val, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(val) == "" {
		return fallback
	}
	switch strings.ToLower(strings.TrimSpace(val)) {
	case "true", "1", "yes":
		return true
	case "false", "0", "no":
		return false
	}
	return fallback
}

// GetListFromEnv returns a comma-separated env var split into a trimmed slice.
// Returns nil (not empty slice) when the var is unset or blank.
func GetListFromEnv(key string) []string {
	val, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(val) == "" {
		return nil
	}
	parts := strings.Split(val, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if t := strings.TrimSpace(p); t != "" {
			out = append(out, t)
		}
	}
	return out
}
