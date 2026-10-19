package sweeper

import (
	"context"
	"log/slog"
	"time"
)

type Expirer interface {
	ExpireReservations(ctx context.Context, now time.Time) (int, error)
}

type Sweeper struct {
	expirer  Expirer
	interval time.Duration
	logger   *slog.Logger
}

type Option func(*Sweeper)

func WithInterval(d time.Duration) Option {
	return func(s *Sweeper) {
		if d > 0 {
			s.interval = d
		}
	}
}

func WithLogger(l *slog.Logger) Option {
	return func(s *Sweeper) {
		if l != nil {
			s.logger = l
		}
	}
}

func New(expirer Expirer, opts ...Option) *Sweeper {
	s := &Sweeper{
		expirer:  expirer,
		interval: 30 * time.Second,
		logger:   slog.Default(),
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

func (s *Sweeper) SweepOnce(ctx context.Context, now time.Time) (int, error) {
	if now.IsZero() {
		now = time.Now()
	}
	now = now.UTC()
	count, err := s.expirer.ExpireReservations(ctx, now)
	if err != nil {
		s.logger.ErrorContext(ctx, "failed to expire reservations", slog.Any("error", err))
		return 0, err
	}
	if count > 0 {
		s.logger.InfoContext(ctx, "expired reservations refunded", slog.Int("count", count))
	}
	return count, nil
}

func (s *Sweeper) Run(ctx context.Context) error {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case t := <-ticker.C:
			_, _ = s.SweepOnce(ctx, t.UTC())
		}
	}
}
