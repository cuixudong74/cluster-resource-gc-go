package gc

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"time"
)

// Default execution limits used when invalid values are supplied.
const (
	DefaultConcurrency = 2
	DefaultBatchSize   = 20
	DefaultMaxAttempts = 3
)

// Executor applies a dry-run plan in bounded, concurrent batches.
type Executor struct {
	Source            Source
	Store             CheckpointStore
	Audit             AuditSink
	Authz             Authorizer
	Concurrency       int
	BatchSize         int
	MaxAttempts       int
	RequestsPerSecond float64
}

// ExecuteResult summarizes applied work and does not double-count recovery.
type ExecuteResult struct {
	Deleted        int64
	AlreadyDeleted int64
	Failed         int64
	Denied         int64
	Skipped        int64
}

// Execute applies only planned deletions and resumes committed progress.
func (e *Executor) Execute(ctx context.Context, plan *Plan, principal Principal) (*ExecuteResult, error) {
	if plan == nil {
		return nil, errors.New("plan is required")
	}
	if e.Source == nil {
		return nil, errors.New("source is required")
	}
	if e.Store == nil {
		return nil, errors.New("checkpoint store is required")
	}
	if e.Authz == nil {
		return nil, errors.New("authorizer is required")
	}

	committed, err := e.Store.LoadPlan(ctx, plan.PlanID)
	if err != nil {
		return nil, fmt.Errorf("load committed plan: %w", err)
	}
	if !plansEqual(committed, plan) {
		return nil, errors.New("in-memory plan does not match committed plan; refusing to execute")
	}
	entries, err := e.Store.List(ctx, plan.PlanID)
	if err != nil {
		return nil, fmt.Errorf("load checkpoints: %w", err)
	}
	latest, err := ReplayCheckpoint(entries, plan)
	if err != nil {
		return nil, err
	}

	items := make([]PlanItem, 0, len(plan.Items))
	for _, item := range plan.Items {
		if item.Action == ActionDelete {
			items = append(items, item)
		}
	}

	result := &ExecuteResult{}
	concurrency := positiveOrDefault(e.Concurrency, DefaultConcurrency)
	batchSize := positiveOrDefault(e.BatchSize, DefaultBatchSize)
	limiter := newRateLimiter(e.RequestsPerSecond)
	var fatalMu sync.Mutex
	var fatal error
	setFatal := func(err error) {
		if err == nil {
			return
		}
		fatalMu.Lock()
		defer fatalMu.Unlock()
		if fatal == nil {
			fatal = err
		}
	}

	for start := 0; start < len(items); start += batchSize {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		if fatal != nil {
			return result, fatal
		}
		end := start + batchSize
		if end > len(items) {
			end = len(items)
		}
		batch := items[start:end]
		sem := make(chan struct{}, concurrency)
		var wg sync.WaitGroup
		for _, item := range batch {
			item := item
			wg.Add(1)
			sem <- struct{}{}
			go func() {
				defer wg.Done()
				defer func() { <-sem }()
				if err := e.executeItem(ctx, plan.PlanID, item, latest[item.ID], principal, limiter, result); err != nil {
					setFatal(err)
				}
			}()
		}
		wg.Wait()
		if fatal != nil {
			return result, fatal
		}
	}
	return result, nil
}

func (e *Executor) executeItem(
	ctx context.Context,
	planID string,
	item PlanItem,
	last CheckpointEntry,
	principal Principal,
	limiter *rateLimiter,
	result *ExecuteResult,
) error {
	if last.ID == item.ID && last.IdempotencyKey != "" && last.IdempotencyKey != item.IdempotencyKey {
		return fmt.Errorf("idempotency key mismatch for %s: refusing to apply old checkpoint to current plan", item.ID)
	}
	if last.Status == StatusDeleted && last.Committed {
		if last.IdempotencyKey != item.IdempotencyKey {
			return fmt.Errorf("committed idempotency key mismatch for %s", item.ID)
		}
		if e.Audit != nil {
			if err := e.Audit.Record(ctx, AuditEvent{
				PlanID:    planID,
				Principal: principal.Name,
				Action:    AuditActionResume,
				ID:        item.ID,
				Allowed:   true,
				Outcome:   AuditOutcomeExists,
				Message:   "already committed in checkpoint",
			}); err != nil {
				return err
			}
		}
		atomic.AddInt64(&result.AlreadyDeleted, 1)
		return nil
	}
	// StatusDenied is an auditable non-terminal result. A later run with an
	// authorized principal re-enters authorization; only StatusDeleted skips.
	decision, err := e.Authz.Authorize(ctx, AuthorizationRequest{
		Principal: principal,
		Action:    AuditActionDelete,
		PlanID:    planID,
		ID:        item.ID,
	})
	if err != nil {
		return err
	}
	if !decision.Allowed {
		atomic.AddInt64(&result.Denied, 1)
		return e.record(ctx, CheckpointEntry{
			PlanID:         planID,
			ID:             item.ID,
			Status:         StatusDenied,
			IdempotencyKey: item.IdempotencyKey,
			Committed:      false,
			Message:        decision.Reason,
		}, AuditEvent{
			PlanID:    planID,
			Principal: principal.Name,
			Action:    AuditActionDelete,
			ID:        item.ID,
			Allowed:   false,
			Outcome:   AuditOutcomeDenied,
			Message:   decision.Reason,
		})
	}
	if e.Audit != nil {
		if err := e.Audit.Record(ctx, AuditEvent{
			PlanID:    planID,
			Principal: principal.Name,
			Action:    AuditActionDelete,
			ID:        item.ID,
			Allowed:   true,
			Outcome:   AuditOutcomeAllowed,
			Message:   decision.Reason,
		}); err != nil {
			return err
		}
	}

	maxAttempts := positiveOrDefault(e.MaxAttempts, DefaultMaxAttempts)
	attempt := 1
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := limiter.Wait(ctx); err != nil {
			return err
		}
		if err := e.Store.Append(ctx, CheckpointEntry{
			PlanID:         planID,
			ID:             item.ID,
			Status:         StatusAttempting,
			Attempt:        attempt,
			IdempotencyKey: item.IdempotencyKey,
			Committed:      false,
		}); err != nil {
			return err
		}

		if e.Audit != nil {
			if err := e.Audit.Record(ctx, AuditEvent{
				PlanID:    planID,
				Principal: principal.Name,
				Action:    AuditActionDelete,
				ID:        item.ID,
				Allowed:   true,
				Outcome:   AuditOutcomeAttempting,
				Message:   fmt.Sprintf("attempt %d", attempt),
			}); err != nil {
				return err
			}
		}
		deleteResult, deleteErr := e.Source.Delete(ctx, DeleteRequest{
			ID:             item.ID,
			Attempt:        attempt,
			IdempotencyKey: item.IdempotencyKey,
		})
		if deleteErr == nil {
			if err := e.Store.Append(ctx, CheckpointEntry{
				PlanID:         planID,
				ID:             item.ID,
				Status:         StatusDeleted,
				Attempt:        attempt,
				IdempotencyKey: item.IdempotencyKey,
				Committed:      true,
			}); err != nil {
				return err
			}
			outcome := AuditOutcomeDeleted
			if deleteResult.AlreadyDeleted {
				outcome = AuditOutcomeExists
			}
			if e.Audit != nil {
				if err := e.Audit.Record(ctx, AuditEvent{
					PlanID:    planID,
					Principal: principal.Name,
					Action:    AuditActionDelete,
					ID:        item.ID,
					Allowed:   true,
					Outcome:   outcome,
				}); err != nil {
					return err
				}
			}
			if deleteResult.AlreadyDeleted {
				atomic.AddInt64(&result.AlreadyDeleted, 1)
			} else {
				atomic.AddInt64(&result.Deleted, 1)
			}
			return nil
		}
		if !isRetryableError(deleteErr) || attempt >= maxAttempts {
			if err := e.Store.Append(ctx, CheckpointEntry{
				PlanID:         planID,
				ID:             item.ID,
				Status:         StatusFailed,
				Attempt:        attempt,
				IdempotencyKey: item.IdempotencyKey,
				Committed:      false,
				Message:        deleteErr.Error(),
			}); err != nil {
				return err
			}
			if e.Audit != nil {
				if err := e.Audit.Record(ctx, AuditEvent{
					PlanID:    planID,
					Principal: principal.Name,
					Action:    AuditActionDelete,
					ID:        item.ID,
					Allowed:   true,
					Outcome:   AuditOutcomeFailed,
					Message:   deleteErr.Error(),
				}); err != nil {
					return err
				}
			}
			atomic.AddInt64(&result.Failed, 1)
			return nil
		}
		if err := e.Store.Append(ctx, CheckpointEntry{
			PlanID:         planID,
			ID:             item.ID,
			Status:         StatusFailed,
			Attempt:        attempt,
			IdempotencyKey: item.IdempotencyKey,
			Committed:      false,
			Message:        deleteErr.Error(),
		}); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(retryDelay(attempt)):
		}
		attempt++
	}
}

func (e *Executor) record(ctx context.Context, checkpoint CheckpointEntry, audit AuditEvent) error {
	if err := e.Store.Append(ctx, checkpoint); err != nil {
		return err
	}
	if e.Audit != nil {
		return e.Audit.Record(ctx, audit)
	}
	return nil
}

func plansEqual(a, b *Plan) bool {
	if a == nil || b == nil {
		return false
	}
	return reflect.DeepEqual(a, b)
}

func positiveOrDefault(value, fallback int) int {
	if value <= 0 {
		return fallback
	}
	return value
}

func isRetryableError(err error) bool {
	// Object-not-found is treated by an idempotent source as success. A source
	// that returns it here exposes a real consistency error, while all ordinary
	// errors are retryable up to MaxAttempts.
	return !errors.Is(err, ErrDeletionNotSupported)
}

func retryDelay(attempt int) time.Duration {
	delay := time.Duration(attempt*20) * time.Millisecond
	if delay > 500*time.Millisecond {
		return 500 * time.Millisecond
	}
	return delay
}

type rateLimiter struct {
	interval time.Duration
	mu       sync.Mutex
	next     time.Time
}

func newRateLimiter(rate float64) *rateLimiter {
	if rate <= 0 {
		return &rateLimiter{}
	}
	return &rateLimiter{interval: time.Duration(float64(time.Second) / rate)}
}

func (r *rateLimiter) Wait(ctx context.Context) error {
	if r == nil || r.interval <= 0 {
		return nil
	}
	r.mu.Lock()
	now := time.Now()
	if r.next.IsZero() || now.After(r.next) {
		r.next = now.Add(r.interval)
		r.mu.Unlock()
		return nil
	}
	wait := r.next.Sub(now)
	r.next = r.next.Add(r.interval)
	r.mu.Unlock()
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
