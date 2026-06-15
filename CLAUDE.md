# brihasai-pkg — Claude Context

The **centralized utility package repository** for brihasai (module `github.com/Brihas-AI/go-pkg`). Holds all shared libraries and integrations to avoid code duplication across `brihasai-platform` and other backend services.

Go 1.25.0 · No internal dependencies on product IP or other brihasai repos.

> **This is one of four repos.** See project-wide details in root `CLAUDE.md`:
> - **`brihasai-core`** — private Go IP (persona · bq · wisdom · sentiment · safety · llm · storage).
> - **`brihasai-platform`** — public Go edge/gateway (imports `go-pkg`).
> - **`brihasai-pkg`** (this repo) — shared packages/drivers.
> - **`brihasai-app`** — frontend (web + mobile).

---

## Directory Layout

```
brihasai-pkg/
├── cache/            Simple in-memory cache wrapper (`go-cache`)
├── constants/        Cross-repo domain constant definitions
├── crypto/           At-rest encryption utilities (AES-GCM)
├── elasticsearch/    Elasticsearch connection and index helpers
├── env/              Environment configuration loader
├── errors/           Standard errors, shim helpers and JSON decoding helpers
├── http/             Structured HTTP client, error mapping, and timeout configurations
├── kafka/            Confluent Kafka producer, consumer, and token helpers
├── logger/           Centralized Logrus-based structured logger with hot-reload level config
├── mongo/            MongoDB connection wrapper
├── postgres/         pgx/v5 PostgreSQL pool initialization and APM instrumentation
├── rabbitmq/         AMQP producer helper
├── redis/            Redis v9 client initialization
├── utils/            General functional helpers
├── worker/           Main background task runner / worker setup
├── go.mod / go.sum   Go module definitions
└── CLAUDE.md         This file
```

---

## Build & Test Commands

```bash
# Build all packages
go build ./...

# Run all unit tests
go test ./...

# Test with race detector
go test -race ./...
```

---

## Invariants & Rules

- **Zero IP**: This package must never contain product IP (prompts, scoring models, persona attributes, etc.). It is strictly for technical drivers and wrappers.
- **Flat Package Directories**: Packages must be structured directly under the root subdirectory (e.g., `logger/log.go`, `errors/errors.go`) without redundant nesting (no `logger/logger/logger.go`).
- **No Circular Imports**: Keep subpackages isolated; dependencies should flow unidirectionally (use interfaces or type parameters if needed).
- **Graceful Nils**: Connection constructors (like `redis` or `postgres`) should gracefully handle empty config/URLs and return nil connection clients without crashing, letting callers decide if they want to fail-fast or run in a stub/provisional mode.
