package gc

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	AuditActionPlan   = "plan"
	AuditActionDelete = "delete"
	AuditActionResume = "resume"
)

const (
	AuditOutcomeAllowed    = "allowed"
	AuditOutcomeDenied     = "denied"
	AuditOutcomeDeleted    = "deleted"
	AuditOutcomeExists     = "already-deleted"
	AuditOutcomeFailed     = "failed"
	AuditOutcomeSkipped    = "skipped"
	AuditOutcomeAttempting = "attempting"
)

// AuditEvent records every request, denial, attempt, and result.
type AuditEvent struct {
	Time      time.Time `json:"time"`
	PlanID    string    `json:"planId,omitempty"`
	Principal string    `json:"principal"`
	Action    string    `json:"action"`
	ID        ObjectID  `json:"id,omitempty"`
	Allowed   bool      `json:"allowed"`
	Outcome   string    `json:"outcome"`
	Message   string    `json:"message,omitempty"`
}

// AuditSink receives an append-only audit trail.
type AuditSink interface {
	Record(context.Context, AuditEvent) error
}

// MemoryAuditSink stores events in memory for tests.
type MemoryAuditSink struct {
	mu     sync.Mutex
	events []AuditEvent
}

// Record appends one event.
func (s *MemoryAuditSink) Record(_ context.Context, event AuditEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if event.Time.IsZero() {
		event.Time = time.Now().UTC()
	}
	s.events = append(s.events, event)
	return nil
}

// Events returns a defensive copy of audit events.
func (s *MemoryAuditSink) Events() []AuditEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]AuditEvent(nil), s.events...)
}

// FileAuditSink appends JSONL events to a file.
type FileAuditSink struct {
	Path string
	mu   sync.Mutex
}

// Record atomically appends one JSON event line.
func (s *FileAuditSink) Record(_ context.Context, event AuditEvent) error {
	if event.Time.IsZero() {
		event.Time = time.Now().UTC()
	}
	b, err := json.Marshal(event)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := os.MkdirAll(filepath.Dir(s.Path), 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(s.Path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.Write(append(b, '\n')); err != nil {
		return err
	}
	return f.Sync()
}

// ReadAuditEvents loads a JSONL audit file.
func ReadAuditEvents(path string) ([]AuditEvent, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	events := make([]AuditEvent, 0)
	for _, line := range splitNonEmptyLines(b) {
		var event AuditEvent
		if err := json.Unmarshal(line, &event); err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	return events, nil
}
