package gc

import (
	"context"
	"testing"
)

func TestExecutorRejectsMismatchedDanglingReferenceContent(t *testing.T) {
	id := ObjectID{Kind: "ConfigMap", Name: "x"}
	source := NewMemorySource(&Snapshot{Objects: []Object{{ID: id}}})
	dir := t.TempDir()
	store := &FileCheckpointStore{Directory: dir}
	committed := newTestPlan(t, id)
	if err := store.SavePlan(context.Background(), committed); err != nil {
		t.Fatal(err)
	}
	changed := *committed
	changed.DanglingReferences = []DanglingReference{{
		From: id,
		To:   ObjectID{Kind: "Secret", Name: "different-missing"},
	}}
	executor := &Executor{
		Source:      source,
		Store:       store,
		Audit:       &MemoryAuditSink{},
		Authz:       allowAllAuthorizer{},
		Concurrency: 1,
		BatchSize:   10,
		MaxAttempts: 1,
	}
	if _, err := executor.Execute(context.Background(), &changed, Principal{Name: "t", Roles: []string{DefaultDeleteRole}}); err == nil {
		t.Fatal("executor must reject plan content that does not match committed plan")
	}
}

func TestReplayCheckpointOnlyTreatsMatchingDeletedAsCommitted(t *testing.T) {
	id := ObjectID{Kind: "ConfigMap", Name: "x"}
	plan := newTestPlan(t, id)
	key := planItem(plan, id).IdempotencyKey

	committed, err := ReplayCheckpoint([]CheckpointEntry{{
		PlanID:         plan.PlanID,
		ID:             id,
		Status:         StatusDeleted,
		IdempotencyKey: key,
		Committed:      true,
	}}, plan)
	if err != nil {
		t.Fatal(err)
	}
	if committed[id].Status != StatusDeleted {
		t.Fatalf("matching deleted record should be committed: %+v", committed)
	}
	retryable, err := ReplayCheckpoint([]CheckpointEntry{{
		PlanID:         plan.PlanID,
		ID:             id,
		Status:         StatusAttempting,
		IdempotencyKey: key,
	}}, plan)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := retryable[id]; ok {
		t.Fatalf("attempting state must be replayed, not skipped: %+v", retryable)
	}
	if _, err := ReplayCheckpoint([]CheckpointEntry{{
		PlanID:         plan.PlanID,
		ID:             id,
		Status:         StatusDeleted,
		IdempotencyKey: "old-key",
		Committed:      true,
	}}, plan); err == nil {
		t.Fatal("old idempotency key must fail replay")
	}
}
