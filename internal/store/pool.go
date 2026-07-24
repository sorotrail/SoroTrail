package store

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	defaultHealthCheckInterval = 15 * time.Second
	defaultInitialBackoff      = 500 * time.Millisecond
	defaultMaxBackoff          = 30 * time.Second
	defaultReconnectTimeout    = 10 * time.Second
)

type PoolManagerOptions struct {
	HealthCheckInterval time.Duration
	InitialBackoff      time.Duration
	MaxBackoff          time.Duration
	ReconnectTimeout    time.Duration
}

func (o PoolManagerOptions) withDefaults() PoolManagerOptions {
	if o.HealthCheckInterval <= 0 {
		o.HealthCheckInterval = defaultHealthCheckInterval
	}
	if o.InitialBackoff <= 0 {
		o.InitialBackoff = defaultInitialBackoff
	}
	if o.MaxBackoff <= 0 {
		o.MaxBackoff = defaultMaxBackoff
	}
	if o.ReconnectTimeout <= 0 {
		o.ReconnectTimeout = defaultReconnectTimeout
	}
	return o
}

var DefaultPoolOptions = PoolManagerOptions{}

type PoolManager struct {
	dsn     string
	pool    atomic.Pointer[pgxpool.Pool]
	healthy atomic.Bool
	log     *slog.Logger
	opts    PoolManagerOptions
}

func NewPoolManager(ctx context.Context, dsn string, log *slog.Logger, opts PoolManagerOptions) (*PoolManager, error) {
	opts = opts.withDefaults()
	p := &PoolManager{
		dsn:  dsn,
		log:  log,
		opts: opts,
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("creating initial pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("pinging initial pool: %w", err)
	}
	p.pool.Store(pool)
	p.healthy.Store(true)
	p.log.Info("postgres pool created")
	return p, nil
}

func (p *PoolManager) Pool() *pgxpool.Pool {
	return p.pool.Load()
}

func (p *PoolManager) Healthy() bool {
	return p.healthy.Load()
}

func (p *PoolManager) Close() {
	if pool := p.pool.Swap(nil); pool != nil {
		pool.Close()
	}
	p.healthy.Store(false)
}

func (p *PoolManager) Run(ctx context.Context) {
	ticker := time.NewTicker(p.opts.HealthCheckInterval)
	defer ticker.Stop()

	var backoff time.Duration

	for {
		select {
		case <-ctx.Done():
			p.log.Info("pool health check stopped")
			return
		case <-ticker.C:
			if err := p.ping(ctx); err != nil {
				p.healthy.Store(false)
				if backoff == 0 {
					backoff = p.opts.InitialBackoff
				}
				p.log.Error("postgres pool health check failed, will reconnect",
					"error", err, "backoff", backoff)

				select {
				case <-ctx.Done():
					return
				case <-time.After(backoff):
				}

				if err := p.reconnect(ctx); err != nil {
					p.log.Error("postgres pool reconnect failed", "error", err)
					backoff = p.nextBackoff(backoff)
				} else {
					p.log.Info("postgres pool reconnected")
					p.healthy.Store(true)
					backoff = 0
				}
			} else {
				if !p.healthy.Load() {
					p.log.Info("postgres pool health restored")
				}
				p.healthy.Store(true)
				backoff = 0
			}
		}
	}
}

func (p *PoolManager) ping(ctx context.Context) error {
	pool := p.Pool()
	if pool == nil {
		return fmt.Errorf("pool is closed")
	}
	pingCtx, cancel := context.WithTimeout(ctx, p.opts.ReconnectTimeout)
	defer cancel()
	return pool.Ping(pingCtx)
}

func (p *PoolManager) reconnect(ctx context.Context) error {
	reconnectCtx, cancel := context.WithTimeout(ctx, p.opts.ReconnectTimeout)
	defer cancel()

	newPool, err := pgxpool.New(reconnectCtx, p.dsn)
	if err != nil {
		return fmt.Errorf("creating new pool: %w", err)
	}

	if err := newPool.Ping(reconnectCtx); err != nil {
		newPool.Close()
		return fmt.Errorf("pinging new pool: %w", err)
	}

	old := p.pool.Swap(newPool)
	if old != nil {
		old.Close()
	}
	return nil
}

func (p *PoolManager) nextBackoff(current time.Duration) time.Duration {
	next := current * 2
	if next > p.opts.MaxBackoff {
		next = p.opts.MaxBackoff
	}
	jitter := time.Duration(rand.Int63n(int64(next / 2)))
	return next/2 + jitter
}
