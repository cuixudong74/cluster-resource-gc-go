package gc

import (
	"context"
	"sync"
	"testing"
	"time"
)

func newTestPlan(t *testing.T, ids ...ObjectID) *Plan {
	t.Helper()
	snapshot := &Snapshot{Objects: make([]Object, 0, len(ids))}
	for _, id := range ids {
		snapshot.Objects = append(snapshot.Objects, Object{ID: id})
	}
	analysis, err := (&Analyzer{}).Analyze(context.Background(), snapshot)
	if err != nil {
		t.Fatal(err)
	}
	planner := &Planner{Policy: Policy{ProtectedLabel: "resource-gc/protected"}}
	plan, err := planner.DryRun(snapshot, analysis)
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func saveTestPlan(t *testing.T, dir string, plan *Plan) {
	t.Helper()
	store := &FileCheckpointStore{Directory: dir}
	if err := store.SavePlan(context.Background(), plan); err != nil {
		t.Fatal(err)
	}
}

func TestExecutorDeniesUnauthorizedAndDeletesAuthorized(t *testing.T) {
	allowed := ObjectID{Kind: "ConfigMap", Namespace: "ns", Name: "allowed"}
	denied := ObjectID{Kind: "ConfigMap", Namespace: "ns", Name: "denied"}
	plan := newTestPlan(t, allowed, denied)
	dir := t.TempDir()
	saveTestPlan(t, dir, plan)
	source := NewMemorySource(snapshotForIDs(allowed, denied))
	audit := &MemoryAuditSink{}
	executor := &Executor{
		Source:      source,
		Store:       &FileCheckpointStore{Directory: dir},
		Audit:       audit,
		Authz:       selectiveAuthorizer{denied: map[ObjectID]bool{denied: true}},
		Concurrency: 2,
		BatchSize:   10,
		MaxAttempts: 1,
	}

	result, err := executor.Execute(context.Background(), plan, Principal{Name: "tester", Roles: []string{"gc-admin"}})
	if err != nil {
		t.Fatal(err)
	}
	if result.Deleted != 1 || result.Denied != 1 {
		t.Fatalf("unexpected result: %+v", result)
	}
	loaded, err := source.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded.Objects) != 1 || loaded.Objects[0].ID != denied {
		t.Fatalf("unauthorized object must remain, got %+v", loaded.Objects)
	}
	var sawDenied bool
	for _, event := range audit.Events() {
		if event.ID == denied && !event.Allowed && event.Outcome == AuditOutcomeDenied {
			sawDenied = true
		}
	}
	if !sawDenied {
		t.Fatalf("missing traceable denial event: %+v", audit.Events())
	}
}

func TestExecutorConcurrentDeletesAndRetryIdempotency(t *testing.T) {
	ids := []ObjectID{
		{Kind: "ConfigMap", Namespace: "ns", Name: "a"},
		{Kind: "ConfigMap", Namespace: "ns", Name: "b"},
		{Kind: "ConfigMap", Namespace: "ns", Name: "c"},
	}
	plan := newTestPlan(t, ids...)
	dir := t.TempDir()
	saveTestPlan(t, dir, plan)
	source := &failFirstSource{
		inner:   NewMemorySource(snapshotForIDs(ids...)),
		failOne: map[string]bool{ids[0].String(): true},
	}
	executor := &Executor{
		Source:            source,
		Store:             &FileCheckpointStore{Directory: dir},
		Audit:             &MemoryAuditSink{},
		Authz:             allowAllAuthorizer{},
		Concurrency:       3,
		BatchSize:         2,
		MaxAttempts:       2,
		RequestsPerSecond: 100,
	}
	result, err := executor.Execute(context.Background(), plan, Principal{Name: "tester", Roles: []string{"gc-admin"}})
	if err != nil {
		t.Fatal(err)
	}
	if result.Deleted != 3 || result.Failed != 0 || result.AlreadyDeleted != 0 {
		t.Fatalf("unexpected result: %+v", result)
	}
	loaded, err := source.inner.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded.Objects) != 0 {
		t.Fatalf("all planned objects should be gone, got %+v", loaded.Objects)
	}
	if source.attempts[ids[0].String()] != 2 {
		t.Fatalf("first object should have one retry, got %d", source.attempts[ids[0].String()])
	}
}

func TestExecutorResumesInterruptedAndCommittedStates(t *testing.T) {
	interrupted := ObjectID{Kind: "ConfigMap", Namespace: "ns", Name: "interrupted"}
	committed := ObjectID{Kind: "ConfigMap", Namespace: "ns", Name: "committed"}
	plan := newTestPlan(t, interrupted, committed)
	dir := t.TempDir()
	store := &FileCheckpointStore{Directory: dir}
	saveTestPlan(t, dir, plan)

	// Simulate a process that wrote "attempting" for interrupted and a durable
	// "deleted" marker for committed before the tests restarts execution.
	if err := store.Append(context.Background(), CheckpointEntry{
		PlanID:         plan.PlanID,
		ID:             interrupted,
		Status:         StatusAttempting,
		Attempt:        1,
		IdempotencyKey: planItem(plan, interrupted).IdempotencyKey,
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Append(context.Background(), CheckpointEntry{
		PlanID:         plan.PlanID,
		ID:             committed,
		Status:         StatusDeleted,
		Attempt:        1,
		IdempotencyKey: planItem(plan, committed).IdempotencyKey,
		Committed:      true,
	}); err != nil {
		t.Fatal(err)
	}

	source := &countingSource{
		inner: NewMemorySource(snapshotForIDs(interrupted, committed)),
		calls: map[string]int{},
	}
	executor := &Executor{
		Source:      source,
		Store:       store,
		Audit:       &MemoryAuditSink{},
		Authz:       allowAllAuthorizer{},
		Concurrency: 1,
		BatchSize:   10,
		MaxAttempts: 1,
	}
	result, err := executor.Execute(context.Background(), plan, Principal{Name: "tester", Roles: []string{"gc-admin"}})
	if err != nil {
		t.Fatal(err)
	}
	if result.Deleted != 1 || result.AlreadyDeleted != 1 {
		t.Fatalf("interrupted should retry, committed should skip: %+v", result)
	}
	if source.calls[interrupted.String()] != 1 {
		t.Fatalf("interrupted item should be attempted once after restart: %d", source.calls[interrupted.String()])
	}
	if source.calls[committed.String()] != 0 {
		t.Fatalf("committed item must not call Delete again: %d", source.calls[committed.String()])
	}
	entries, err := store.List(context.Background(), plan.PlanID)
	if err != nil {
		t.Fatal(err)
	}
	latest := LatestCheckpoint(entries)
	if latest[interrupted].Status != StatusDeleted || !latest[interrupted].Committed {
		t.Fatalf("interrupted item should end committed-deleted: %+v", latest[interrupted])
	}
}

func snapshotForIDs(ids ...ObjectID) *Snapshot {
	snapshot := &Snapshot{Objects: make([]Object, 0, len(ids))}
	for _, id := range ids {
		snapshot.Objects = append(snapshot.Objects, Object{ID: id})
	}
	return snapshot
}

type allowAllAuthorizer struct{}

func (allowAllAuthorizer) Authorize(context.Context, AuthorizationRequest) (AuthorizationDecision, error) {
	return AuthorizationDecision{Allowed: true, Reason: "allowed in test"}, nil
}

type selectiveAuthorizer struct {
	denied map[ObjectID]bool
}

func (a selectiveAuthorizer) Authorize(_ context.Context, req AuthorizationRequest) (AuthorizationDecision, error) {
	if a.denied[req.ID] {
		return AuthorizationDecision{Allowed: false, Reason: "test denial"}, nil
	}
	return AuthorizationDecision{Allowed: true, Reason: "allowed in test"}, nil
}

type failFirstSource struct {
	mu       sync.Mutex
	inner    *MemorySource
	failOne  map[string]bool
	attempts map[string]int
}

func (s *failFirstSource) Load(ctx context.Context) (*Snapshot, error) {
	return s.inner.Load(ctx)
}

func (s *failFirstSource) Delete(ctx context.Context, req DeleteRequest) (DeleteResult, error) {
	s.mu.Lock()
	if s.attempts == nil {
		s.attempts = map[string]int{}
	}
	key := req.ID.String()
	s.attempts[key]++
	shouldFail := s.failOne[key] && s.attempts[key] == 1
	s.mu.Unlock()
	if shouldFail {
		return DeleteResult{}, context.DeadlineExceeded
	}
	return s.inner.Delete(ctx, req)
}

type countingSource struct {
	mu    sync.Mutex
	inner *MemorySource
	calls map[string]int
}

func (s *countingSource) Load(ctx context.Context) (*Snapshot, error) {
	return s.inner.Load(ctx)
}

func (s *countingSource) Delete(ctx context.Context, req DeleteRequest) (DeleteResult, error) {
	s.mu.Lock()
	s.calls[req.ID.String()]++
	s.mu.Unlock()
	return s.inner.Delete(ctx, req)
}

var _ = time.Second

func TestExecutorDenialIsRecoverableAfterAuthorizationChanges(t *testing.T) {
	id := ObjectID{Kind: "ConfigMap", Namespace: "ns", Name: "denied-then-allowed"}
	plan := newTestPlan(t, id)
	dir := t.TempDir()
	store := &FileCheckpointStore{Directory: dir}
	saveTestPlan(t, dir, plan)
	source := NewMemorySource(snapshotForIDs(id))

	deniedExecutor := &Executor{
		Source:      source,
		Store:       store,
		Audit:       &MemoryAuditSink{},
		Authz:       selectiveAuthorizer{denied: map[ObjectID]bool{id: true}},
		Concurrency: 1,
		BatchSize:   10,
		MaxAttempts: 1,
	}
	first, err := deniedExecutor.Execute(context.Background(), plan, Principal{Name: "weak-user"})
	if err != nil {
		t.Fatal(err)
	}
	if first.Denied != 1 || first.Deleted != 0 {
		t.Fatalf("unexpected first result: %+v", first)
	}

	allowedExecutor := &Executor{
		Source:      source,
		Store:       store,
		Audit:       &MemoryAuditSink{},
		Authz:       allowAllAuthorizer{},
		Concurrency: 1,
		BatchSize:   10,
		MaxAttempts: 1,
	}
	second, err := allowedExecutor.Execute(context.Background(), plan, Principal{Name: "admin", Roles: []string{DefaultDeleteRole}})
	if err != nil {
		t.Fatal(err)
	}
	if second.Deleted != 1 || second.Denied != 0 {
		t.Fatalf("authorized recovery should delete previously denied item: %+v", second)
	}
	entries, err := store.List(context.Background(), plan.PlanID)
	if err != nil {
		t.Fatal(err)
	}
	latest := LatestCheckpoint(entries)
	if latest[id].Status != StatusDeleted || !latest[id].Committed {
		t.Fatalf("expected final committed deleted state, got %+v", latest[id])
	}
}

func TestExecutorRecoversDeleteThatSucceededBeforeCancel(t *testing.T) {
	id := ObjectID{Kind: "ConfigMap", Namespace: "ns", Name: "deleted-before-marker"}
	plan := newTestPlan(t, id)
	dir := t.TempDir()
	store := &FileCheckpointStore{Directory: dir}
	saveTestPlan(t, dir, plan)
	if err := store.Append(context.Background(), CheckpointEntry{
		PlanID:         plan.PlanID,
		ID:             id,
		Status:         StatusAttempting,
		Attempt:        1,
		IdempotencyKey: planItem(plan, id).IdempotencyKey,
	}); err != nil {
		t.Fatal(err)
	}
	// The source already deleted the object before interruption prevented the
	// deleted checkpoint and result event from being recorded.
	source := NewMemorySource(&Snapshot{})
	executor := &Executor{
		Source:      source,
		Store:       store,
		Audit:       &MemoryAuditSink{},
		Authz:       allowAllAuthorizer{},
		Concurrency: 1,
		BatchSize:   10,
		MaxAttempts: 1,
	}
	result, err := executor.Execute(context.Background(), plan, Principal{Name: "admin", Roles: []string{DefaultDeleteRole}})
	if err != nil {
		t.Fatal(err)
	}
	if result.Deleted != 0 || result.AlreadyDeleted != 1 {
		t.Fatalf("replayed source success should count once as already-deleted: %+v", result)
	}
	entries, err := store.List(context.Background(), plan.PlanID)
	if err != nil {
		t.Fatal(err)
	}
	if LatestCheckpoint(entries)[id].Status != StatusDeleted {
		t.Fatalf("recovery must commit deleted marker: %+v", LatestCheckpoint(entries)[id])
	}
}
