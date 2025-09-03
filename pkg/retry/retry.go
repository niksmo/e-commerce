package retry

import (
	"context"
	"fmt"
	"math/rand/v2"
	"time"
)

const defaultDelay = 100 * time.Millisecond

type Backoff func(attempt int) time.Duration

type Settings struct {
	MaxAttempts int
	Backoff     Backoff
	ShouldRetry func(error) bool
}

func (s *Settings) normalize() {
	if s.MaxAttempts == 0 {
		s.MaxAttempts = 1
	}

	if s.Backoff == nil {
		s.Backoff = defaultBackoff()
	}

	if s.ShouldRetry == nil {
		s.ShouldRetry = alwaysRetry
	}

}

func defaultBackoff() Backoff {
	return ExponentialBackoff(defaultDelay)
}

func alwaysRetry(error) bool {
	return true
}

func ExponentialBackoff(delay time.Duration) Backoff {
	return func(attempt int) time.Duration {
		base := 1 << attempt * delay
		jitter := time.Duration(rand.IntN(int(base/2)) + 1)
		return base + jitter
	}
}

func Do(ctx context.Context, s Settings, fn func() error) error {
	_, err := DoWithResult(ctx, s, func() (struct{}, error) {
		return struct{}{}, fn()
	})
	return err
}

func DoWithResult[T any](ctx context.Context, s Settings, fn func() (T, error)) (T, error) {
	var (
		zero, result T
		err          error
	)

	err = ctx.Err()
	if err != nil {
		return zero, err
	}

	s.normalize()
	timer := time.NewTimer(0)

	for attempt := 1; attempt <= s.MaxAttempts; attempt++ {
		result, err = fn()
		if err == nil || !s.ShouldRetry(err) {
			return result, nil
		}

		wait := s.Backoff(attempt)
		timer.Reset(wait)
		select {
		case <-ctx.Done():
			return zero, fmt.Errorf("%w: %w", ctx.Err(), err)
		case <-timer.C:
		}
	}

	return zero, err
}
