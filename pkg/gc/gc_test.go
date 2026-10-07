package gc

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// writeSnapshot 把快照写入临时文件。
func writeSnapshot(t *testing.T, snap *Snapshot) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "snapshot.json")
	data, err := json.Marshal(snap)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestEndToEndDryRunThenRun(t *testing.T) {
	snap := &Snapshot{Resources: []Resource{
		res("root", "Namespace", ""),
		res("live", "Pod", "root"),
		res("garbage1", "Pod", ""),
		res("garbage2", "ConfigMap", ""),
	}}
	path := writeSnapshot(t, snap)
	dir := t.TempDir()
	deleter := NewMemoryDeleter([]ObjectID{"root", "live", "garbage1", "garbage2"})
	auditor := &MemoryAuditor{}

	g := &GC{
		Source:  &FileSource{Path: path},
		Deleter: deleter,
		Store:   &FileCheckpointStore{Dir: dir},
		Auditor: auditor,
		Auth:    authAll(),
		Config: Config{
			Roots:  RootSet{RootIDs: []ObjectID{"root"}},
			Policy: ProtectPolicy{Now: testNow},
			Plan: PlanOptions{
				BatchSize: 1,
				Classify:  ClassifyOptions{Now: testNow},
			},
			Executor: ExecutorOptions{Concurrency: 2},
		},
	}
	ctx := WithActor(context.Background(), "e2e")

	// 干跑：只出计划，不删除。
	plan, err := g.DryRun(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Items) != 2 {
		t.Fatalf("plan items=%d want 2", len(plan.Items))
	}
	if len(deleter.Deleted) != 0 {
		t.Fatal("dry-run must not delete")
	}

	// 再干跑一次：计划必须完全一致。
	plan2, err := g.DryRun(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if plan.ID != plan2.ID {
		t.Fatalf("plan not reproducible: %s vs %s", plan.ID, plan2.ID)
	}

	// 执行：结果与计划对齐。
	plan3, res, err := g.Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if plan3.ID != plan.ID {
		t.Fatal("run plan differs from dry-run plan")
	}
	if len(res.Deleted) != 2 {
		t.Fatalf("deleted=%d want 2", len(res.Deleted))
	}
	for _, id := range res.Deleted {
		if id != "garbage1" && id != "garbage2" {
			t.Fatalf("unexpected deletion %s", id)
		}
	}
}

func TestEndToEndRestartRecovery(t *testing.T) {
	var resources []Resource
	resources = append(resources, res("root", "Namespace", ""))
	ids := []ObjectID{}
	for _, id := range []ObjectID{"g1", "g2", "g3", "g4"} {
		resources = append(resources, res(id, "Pod", ""))
		ids = append(ids, id)
	}
	snap := &Snapshot{Resources: resources}
	path := writeSnapshot(t, snap)
	dir := t.TempDir()

	newGC := func(d Deleter) *GC {
		return &GC{
			Source:  &FileSource{Path: path},
			Deleter: d,
			Store:   &FileCheckpointStore{Dir: dir},
			Auditor: &MemoryAuditor{},
			Auth:    authAll(),
			Config: Config{
				Roots:  RootSet{RootIDs: []ObjectID{"root"}},
				Policy: ProtectPolicy{Now: testNow},
				Plan: PlanOptions{
					BatchSize: 1,
					Classify:  ClassifyOptions{Now: testNow},
				},
				Executor: ExecutorOptions{Concurrency: 1},
			},
		}
	}

	// 第一次运行：删除 2 个后“进程崩溃”（deleter 永久报错模拟中断）。
	deleter1 := NewMemoryDeleter(ids)
	crashed := &crashDeleter{inner: deleter1, crashAfter: 2}
	ctx := WithActor(context.Background(), "e2e")
	plan1, res1, err := newGC(crashed).Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(res1.Deleted) != 2 {
		t.Fatalf("first run deleted=%d want 2", len(res1.Deleted))
	}
	if len(res1.Failed) != 2 {
		t.Fatalf("first run failed=%d want 2", len(res1.Failed))
	}

	// “进程重启”：新的 GC 实例、新的 deleter（已删对象不在新集群里），
	// 从文件检查点恢复，已提交的不重复删除。
	remaining := []ObjectID{}
	for _, id := range ids {
		remaining = append(remaining, id)
	}
	deleter2 := NewMemoryDeleter(remaining)
	// 新 deleter 里前两个已不存在（模拟已被第一次运行删除）。
	for _, id := range res1.Deleted {
		_ = deleter2.Delete(ctx, id)
	}
	deleter2.Deleted = nil // 清空记录，只统计第二次运行的删除

	plan2, res2, err := newGC(deleter2).Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if plan2.ID != plan1.ID {
		t.Fatal("plan ID changed across restart with same input")
	}
	if res2.Resumed != 2 {
		t.Fatalf("resumed=%d want 2", res2.Resumed)
	}
	if len(res2.Deleted) != 2 {
		t.Fatalf("second run deleted=%d want 2", len(res2.Deleted))
	}
	// 两次运行删除的并集应恰好是全部 4 个垃圾对象，无重复。
	seen := map[ObjectID]int{}
	for _, id := range res1.Deleted {
		seen[id]++
	}
	for _, id := range res2.Deleted {
		seen[id]++
	}
	if len(seen) != 4 {
		t.Fatalf("union of deleted=%v want 4 distinct", seen)
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("object %s deleted %d times", id, n)
		}
	}
}

type crashDeleter struct {
	inner      *MemoryDeleter
	crashAfter int
	n          int
}

func (c *crashDeleter) Delete(ctx context.Context, id ObjectID) error {
	if c.n >= c.crashAfter {
		return errors.New("simulated crash")
	}
	c.n++
	return c.inner.Delete(ctx, id)
}

func TestMemorySourceNoCluster(t *testing.T) {
	// 验证 MemorySource 完全离线可用。
	src := &MemorySource{Snap: Snapshot{
		TakenAt:   time.Now(),
		Resources: []Resource{res("root", "Namespace", "")},
	}}
	snap, err := src.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.Resources) != 1 {
		t.Fatal("memory source broken")
	}
}
