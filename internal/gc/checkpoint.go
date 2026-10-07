package gc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// CheckpointStatus distinguishes committed work from an interrupted attempt.
type CheckpointStatus string

const (
	StatusPlanned    CheckpointStatus = "planned"
	StatusAttempting CheckpointStatus = "attempting"
	StatusDeleted    CheckpointStatus = "deleted"
	StatusFailed     CheckpointStatus = "failed"
	StatusDenied     CheckpointStatus = "denied"
)

// CheckpointEntry is one durable state-transition record.
type CheckpointEntry struct {
	PlanID         string           `json:"planId"`
	ID             ObjectID         `json:"id"`
	Status         CheckpointStatus `json:"status"`
	Attempt        int              `json:"attempt"`
	IdempotencyKey string           `json:"idempotencyKey"`
	Committed      bool             `json:"committed"`
	Message        string           `json:"message,omitempty"`
	RecordedAt     time.Time        `json:"recordedAt"`
}

// CheckpointStore persists plan progress durably.
type CheckpointStore interface {
	LoadPlan(context.Context, string) (*Plan, error)
	SavePlan(context.Context, *Plan) error
	Append(context.Context, CheckpointEntry) error
	List(context.Context, string) ([]CheckpointEntry, error)
}

// FileCheckpointStore is an atomic plan file plus append-only JSONL event log.
type FileCheckpointStore struct {
	Directory string
	mu        sync.Mutex
}

func (s *FileCheckpointStore) planPath(planID string) string {
	return filepath.Join(s.Directory, planID+".json")
}

func (s *FileCheckpointStore) eventsPath(planID string) string {
	return filepath.Join(s.Directory, planID+".events.jsonl")
}

// LoadPlan reads a previously committed plan.
func (s *FileCheckpointStore) LoadPlan(_ context.Context, planID string) (*Plan, error) {
	b, err := os.ReadFile(s.planPath(planID))
	if err != nil {
		return nil, err
	}
	var plan Plan
	if err := json.Unmarshal(b, &plan); err != nil {
		return nil, err
	}
	if plan.PlanID != planID {
		return nil, fmt.Errorf("checkpoint plan id mismatch: got %q want %q", plan.PlanID, planID)
	}
	return &plan, nil
}

// SavePlan atomically commits a dry-run plan before mutation starts.
func (s *FileCheckpointStore) SavePlan(_ context.Context, plan *Plan) error {
	if plan == nil || plan.PlanID == "" {
		return errors.New("plan and plan id are required")
	}
	if err := os.MkdirAll(s.Directory, 0o755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(plan, "", "  ")
	if err != nil {
		return err
	}
	final := s.planPath(plan.PlanID)
	tmp := final + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(append(b, '\n')); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, final); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return syncDir(s.Directory)
}

// Append durably appends one state-transition record.
func (s *FileCheckpointStore) Append(_ context.Context, entry CheckpointEntry) error {
	if entry.PlanID == "" {
		return errors.New("plan id is required")
	}
	if entry.RecordedAt.IsZero() {
		entry.RecordedAt = time.Now().UTC()
	}
	b, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := os.MkdirAll(s.Directory, 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(s.eventsPath(entry.PlanID), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.Write(append(b, '\n')); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	return nil
}

// List reads checkpoint records in their committed append order.
func (s *FileCheckpointStore) List(_ context.Context, planID string) ([]CheckpointEntry, error) {
	b, err := os.ReadFile(s.eventsPath(planID))
	if errors.Is(err, os.ErrNotExist) {
		return []CheckpointEntry{}, nil
	}
	if err != nil {
		return nil, err
	}
	entries := make([]CheckpointEntry, 0)
	for i, line := range splitNonEmptyLines(b) {
		var entry CheckpointEntry
		if err := json.Unmarshal(line, &entry); err != nil {
			return nil, fmt.Errorf("checkpoint line %d: %w", i+1, err)
		}
		if entry.PlanID != planID {
			return nil, fmt.Errorf("checkpoint line %d: unexpected plan id %q", i+1, entry.PlanID)
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

// LatestCheckpoint returns the latest record for each object in stable order.
func LatestCheckpoint(entries []CheckpointEntry) map[ObjectID]CheckpointEntry {
	latest := make(map[ObjectID]CheckpointEntry, len(entries))
	for _, entry := range entries {
		latest[entry.ID] = entry
	}
	return latest
}

// ReplayCheckpoint reconstructs each object's terminal state for a plan.
// Only StatusDeleted with the matching current key is terminal. Attempting,
// failed, and denied records are replayable work and are intentionally absent
// from the returned map.
func ReplayCheckpoint(entries []CheckpointEntry, plan *Plan) (map[ObjectID]CheckpointEntry, error) {
	keys := make(map[ObjectID]string)
	if plan != nil {
		for _, item := range plan.Items {
			keys[item.ID] = item.IdempotencyKey
		}
	}
	latest := make(map[ObjectID]CheckpointEntry)
	for _, entry := range entries {
		expected, known := keys[entry.ID]
		if known && expected != "" && entry.IdempotencyKey != "" && expected != entry.IdempotencyKey {
			return nil, fmt.Errorf("checkpoint idempotency key mismatch for %s", entry.ID)
		}
		latest[entry.ID] = entry
	}
	for id, entry := range latest {
		if entry.Status == StatusDeleted && entry.Committed &&
			(keys[id] == "" || entry.IdempotencyKey == keys[id]) {
			continue
		}
		delete(latest, id)
	}
	return latest, nil
}

// SortedCheckpointEntries is a test/debug helper for deterministic inspection.
func SortedCheckpointEntries(entries []CheckpointEntry) []CheckpointEntry {
	out := append([]CheckpointEntry(nil), entries...)
	sort.SliceStable(out, func(i, j int) bool {
		left, right := out[i], out[j]
		if left.ID.String() != right.ID.String() {
			return left.ID.String() < right.ID.String()
		}
		if left.Attempt != right.Attempt {
			return left.Attempt < right.Attempt
		}
		return left.RecordedAt.Before(right.RecordedAt)
	})
	return out
}

func splitNonEmptyLines(b []byte) [][]byte {
	lines := make([][]byte, 0)
	start := 0
	for i, c := range b {
		if c == '\n' {
			if i > start {
				lines = append(lines, b[start:i])
			}
			start = i + 1
		}
	}
	if start < len(b) {
		lines = append(lines, b[start:])
	}
	return lines
}

func syncDir(path string) error {
	d, err := os.Open(path)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
