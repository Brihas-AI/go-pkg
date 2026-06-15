package postgres

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	log "github.com/sirupsen/logrus"
	"go.elastic.co/apm/module/apmpgxv5/v2"

	"github.com/Brihas-AI/go-pkg/env"
)

var (
	once           sync.Once
	postgresClient *Client
	ErrNotConfigured = errors.New("postgres: not configured")
)

// Client wraps the pgxpool connection pool.
type Client struct {
	Pool *pgxpool.Pool
}

// Config holds the postgres configuration parameters.
type Config struct {
	DatabaseURL       string
	MaxConns          int32
	MinConns          int32
	MaxConnLifetime   time.Duration
	MaxConnIdleTime   time.Duration
	HealthCheckPeriod time.Duration
}

// InitPostgres initializes the global Postgres singleton.
func InitPostgres() {
	once.Do(func() {
		dbURL := env.GetEnvOrDefault("DATABASE_URL", "")
		if dbURL == "" {
			host := env.GetEnvOrDefault("POSTGRES_HOST", "localhost")
			port := env.GetEnvOrDefault("POSTGRES_PORT", "5432")
			user := env.GetEnvOrDefault("POSTGRES_USER", "postgres")
			password := env.GetEnvOrDefault("POSTGRES_PASSWORD", "")
			dbName := env.GetEnvOrDefault("POSTGRES_DB", "postgres")
			sslMode := env.GetEnvOrDefault("POSTGRES_SSLMODE", "disable")

			if password != "" {
				dbURL = fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=%s", user, password, host, port, dbName, sslMode)
			} else {
				dbURL = fmt.Sprintf("postgres://%s@%s:%s/%s?sslmode=%s", user, host, port, dbName, sslMode)
			}
		}

		cfg := Config{
			DatabaseURL:       dbURL,
			MaxConns:          int32(env.GetEnvOrDefaultInt("POSTGRES_MAX_CONNS", 10)),
			MinConns:          int32(env.GetEnvOrDefaultInt("POSTGRES_MIN_CONNS", 1)),
			MaxConnLifetime:   time.Duration(env.GetEnvOrDefaultInt("POSTGRES_MAX_CONN_LIFETIME_MINS", 30)) * time.Minute,
			MaxConnIdleTime:   time.Duration(env.GetEnvOrDefaultInt("POSTGRES_MAX_CONN_IDLE_MINS", 5)) * time.Minute,
			HealthCheckPeriod: time.Duration(env.GetEnvOrDefaultInt("POSTGRES_HEALTHCHECK_SECS", 30)) * time.Second,
		}

		client, err := NewClient(cfg)
		if err != nil {
			log.WithFields(log.Fields{
				"error":  err.Error(),
				"source": "postgres.InitPostgres",
			}).Fatal("[Postgres] init failed")
		}
		postgresClient = client
		fmt.Println("[Postgres] client initialized successfully")
	})
}

// GetClient returns the global Postgres client singleton.
func GetClient() *Client {
	return postgresClient
}

// NewClient creates a new Postgres client. Returns an empty Client wrapper
// (with nil Pool) when databaseURL is empty, allowing graceful degradation.
func NewClient(cfg Config) (*Client, error) {
	if cfg.DatabaseURL == "" {
		return &Client{Pool: nil}, nil
	}

	pgxCfg, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("postgres: parse config: %w", err)
	}

	// Attach APM query tracer
	apmpgxv5.Instrument(pgxCfg.ConnConfig)

	// Apply connection pool limits
	if cfg.MaxConns > 0 {
		pgxCfg.MaxConns = cfg.MaxConns
	} else {
		pgxCfg.MaxConns = 10
	}
	if cfg.MinConns > 0 {
		pgxCfg.MinConns = cfg.MinConns
	} else {
		pgxCfg.MinConns = 1
	}
	if cfg.MaxConnLifetime > 0 {
		pgxCfg.MaxConnLifetime = cfg.MaxConnLifetime
	} else {
		pgxCfg.MaxConnLifetime = 30 * time.Minute
	}
	if cfg.MaxConnIdleTime > 0 {
		pgxCfg.MaxConnIdleTime = cfg.MaxConnIdleTime
	} else {
		pgxCfg.MaxConnIdleTime = 5 * time.Minute
	}
	if cfg.HealthCheckPeriod > 0 {
		pgxCfg.HealthCheckPeriod = cfg.HealthCheckPeriod
	} else {
		pgxCfg.HealthCheckPeriod = 30 * time.Second
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := pgxpool.NewWithConfig(ctx, pgxCfg)
	if err != nil {
		return nil, fmt.Errorf("postgres: connect: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("postgres: ping: %w", err)
	}

	return &Client{Pool: pool}, nil
}

// Close releases the Postgres connection pool. Safe to call on nil.
func (c *Client) Close() {
	if c == nil || c.Pool == nil {
		return
	}
	c.Pool.Close()
}

// Available reports whether the pool is initialized and active.
func (c *Client) Available() bool {
	return c != nil && c.Pool != nil
}

// Ping checks pool liveness. Returns "ok", "error:<msg>", or "not_configured".
func (c *Client) Ping(ctx context.Context) string {
	if !c.Available() {
		return "not_configured"
	}
	pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err := c.Pool.Ping(pingCtx); err != nil {
		return "error:" + truncate(err.Error(), 120)
	}
	return "ok"
}

// Row is the minimal Scan interface. pgx.Row satisfies it directly.
type Row interface {
	Scan(dest ...any) error
}

type errRow struct{ err error }

func (e errRow) Scan(_ ...any) error { return e.err }

// QueryRow runs a single-row query. Returns pgx.ErrNoRows when no row matched.
func (c *Client) QueryRow(ctx context.Context, sql string, args ...any) Row {
	if !c.Available() {
		return errRow{err: ErrNotConfigured}
	}
	return c.Pool.QueryRow(ctx, sql, args...)
}

// Exec runs a command and returns rows affected.
func (c *Client) Exec(ctx context.Context, sql string, args ...any) (int64, error) {
	if !c.Available() {
		return 0, ErrNotConfigured
	}
	tag, err := c.Pool.Exec(ctx, sql, args...)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

// Query runs a multi-row query.
func (c *Client) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if !c.Available() {
		return nil, ErrNotConfigured
	}
	return c.Pool.Query(ctx, sql, args...)
}

// RequireMigrations fails fast if any of the named migrations are missing.
func (c *Client) RequireMigrations(ctx context.Context, names ...string) error {
	if !c.Available() {
		return nil
	}
	if len(names) == 0 {
		return nil
	}

	probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	var tableOK bool
	err := c.Pool.QueryRow(probeCtx,
		`SELECT EXISTS (
			SELECT 1 FROM information_schema.tables
			 WHERE table_schema = current_schema()
			   AND table_name   = 'schema_migrations'
		)`,
	).Scan(&tableOK)
	if err != nil {
		return fmt.Errorf("schema_migrations probe failed: %w", err)
	}
	if !tableOK {
		return errors.New("schema_migrations table missing — apply migrations 001_schema.sql and onwards via deploy/cloud_sql_init.sh before starting the gateway")
	}

	rows, err := c.Pool.Query(probeCtx,
		`SELECT migration_name
		   FROM schema_migrations
		  WHERE migration_name = ANY($1::text[])`,
		names,
	)
	if err != nil {
		return fmt.Errorf("schema_migrations read failed: %w", err)
	}
	defer rows.Close()

	found := make(map[string]struct{}, len(names))
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err == nil {
			found[n] = struct{}{}
		}
	}

	var missing []string
	for _, want := range names {
		if _, ok := found[want]; !ok {
			missing = append(missing, want)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("required migrations not applied: %v — run deploy/cloud_sql_init.sh against the target database before starting the gateway", missing)
	}
	return nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
