package gc

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func writeSnapshot(t *testing.T, path string, snapshot *Snapshot) {
	t.Helper()
	b, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestRunDryRunThenExecuteAndResume(t *testing.T) {
	now := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
	rootID := ObjectID{Kind: "Namespace", Name: "live"}
	garbageID := ObjectID{Kind: "ConfigMap", Namespace: "live", Name: "old"}
	snapshot := &Snapshot{
		GeneratedAt: now,
		Objects: []Object{
			{ID: rootID, Root: true},
			{ID: garbageID},
		},
	}
	dir := t.TempDir()
	snapshotPath := filepath.Join(dir, "snapshot.json")
	writeSnapshot(t, snapshotPath, snapshot)
	cfg := func() *Config {
		return &Config{
			Source:            SourceMemoryFromFile,
			SnapshotPath:      snapshotPath,
			StateDir:          filepath.Join(dir, "state"),
			AuditPath:         filepath.Join(dir, "audit.jsonl"),
			Concurrency:       1,
			BatchSize:         10,
			MaxAttempts:       1,
			RequestsPerSecond: 100,
			Timeout:           Duration(time.Minute),
			Principal:         Principal{Name: "bot", Roles: []string{DefaultDeleteRole}},
		}
	}

	dryCfg := cfg()
	dryCfg.DryRun = true
	plan, result, err := Run(context.Background(), dryCfg)
	if err != nil {
		t.Fatal(err)
	}
	if result != nil {
		t.Fatalf("dry run should not execute: %+v", result)
	}
	if item := planItem(plan, garbageID); item.Action != ActionDelete {
		t.Fatalf("dry run should plan delete: %+v", item)
	}

	execCfg := cfg()
	execCfg.DryRun = false
	execPlan, execResult, err := Run(context.Background(), execCfg)
	if err != nil {
		t.Fatal(err)
	}
	if execPlan.PlanID != plan.PlanID {
		t.Fatalf("execute created a different plan id")
	}
	if execResult.Deleted != 1 || execResult.AlreadyDeleted != 0 {
		t.Fatalf("unexpected execute result: %+v", execResult)
	}

	resumeCfg := cfg()
	resumeCfg.DryRun = false
	resumeCfg.ResumePlanID = plan.PlanID
	_, resumeResult, err := Run(context.Background(), resumeCfg)
	if err != nil {
		t.Fatal(err)
	}
	if resumeResult.Deleted != 0 || resumeResult.AlreadyDeleted != 1 {
		t.Fatalf("resume should skip committed delete: %+v", resumeResult)
	}
	events, err := ReadAuditEvents(execCfg.AuditPath)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) < 3 {
		t.Fatalf("expected allow/delete/resume audit trail, got %+v", events)
	}
}

func TestRunRejectsResumeAfterInputChanges(t *testing.T) {
	now := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
	snapshot := &Snapshot{Objects: []Object{{ID: ObjectID{Kind: "ConfigMap", Name: "old"}}}}
	dir := t.TempDir()
	path := filepath.Join(dir, "snapshot.json")
	writeSnapshot(t, path, snapshot)
	cfg := &Config{
		Source:            SourceMemoryFromFile,
		SnapshotPath:      path,
		StateDir:          filepath.Join(dir, "state"),
		AuditPath:         filepath.Join(dir, "audit.jsonl"),
		Concurrency:       1,
		BatchSize:         10,
		MaxAttempts:       1,
		RequestsPerSecond: 100,
		Timeout:           Duration(time.Minute),
		Principal:         Principal{Name: "bot", Roles: []string{DefaultDeleteRole}},
	}
	plan, _, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	writeSnapshot(t, path, &Snapshot{GeneratedAt: now, Objects: []Object{{ID: ObjectID{Kind: "ConfigMap", Name: "changed"}}}})
	cfg.ResumePlanID = plan.PlanID
	if _, _, err := Run(context.Background(), cfg); err != ErrPlanInputChanged {
		t.Fatalf("expected ErrPlanInputChanged, got %v", err)
	}
}
