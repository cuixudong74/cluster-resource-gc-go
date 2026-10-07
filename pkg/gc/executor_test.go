package gc

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// newTestPlan 构造一个包含 n 个待回收对象的计划。
func newTestPlan(t *testing.T, n, batchSize int) (*Plan, *MemoryDeleter) {
	t.Helper()
	resources := []Resource{res("root", "Namespace", "")}
	ids := []ObjectID{}
	for i := 0; i < n; i++ {
		id := ObjectID(fmt.Sprintf("obj-%03d", i))
		resources = append(resources, res(id, "Pod", ""))
		ids = append(ids, id)
	}
	snap := &Snapshot{Resources: resources}
	plan, err := BuildPlan(snap, RootSet{RootIDs: []ObjectID{"root"}}, PlanOptions{
		BatchSize: batchSize,
		Policy:    &ProtectPolicy{Now: testNow},
		Classify:  ClassifyOptions{Now: testNow},
	})
	if err != nil {
		t.Fatal(err)
	}
	return plan, NewMemoryDeleter(ids)
}

func authAll() *StaticAuthorizer {
	return &StaticAuthorizer{Allowed: map[string]bool{"*": true}}
}

func TestExecuteDeletesExactlyPlanned(t *testing.T) {
	plan, deleter := newTestPlan(t, 10, 3)
	auditor := &MemoryAuditor{}
	exec := &Executor{
		Deleter: deleter, Store: NewMemoryCheckpointStore(), Auditor: auditor,
		Auth: authAll(),
		Opts: ExecutorOptions{Concurrency: 4},
	}
	ctx := WithActor(context.Background(), "tester")
	res, err := exec.Execute(ctx, plan)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Deleted) != 10 {
		t.Fatalf("deleted=%d want 10", len(res.Deleted))
	}
	// 结果与计划严格对齐：无计划外删除。
	planned := map[ObjectID]bool{}
	for _, it := range plan.Items {
		planned[it.ID] = true
	}
	for _, id := range deleter.Deleted {
		if !planned[id] {
			t.Fatalf("unplanned deletion: %s", id)
		}
	}
	// 每次删除都有审计记录。
	var delRecords int
	for _, r := range auditor.All() {
		if r.Action == ActionDelete {
			delRecords++
		}
	}
	if delRecords != 10 {
		t.Fatalf("audit delete records=%d want 10", delRecords)
	}
}

func TestExecuteConcurrencyLimit(t *testing.T) {
	plan, _ := newTestPlan(t, 20, 20)
	var cur, max atomic.Int32
	deleter := &trackingDeleter{
		onDelete: func() {
			c := cur.Add(1)
			for {
				m := max.Load()
				if c <= m || max.CompareAndSwap(m, c) {
					break
				}
			}
			time.Sleep(5 * time.Millisecond)
			cur.Add(-1)
		},
	}
	exec := &Executor{
		Deleter: deleter, Store: NewMemoryCheckpointStore(), Auth: authAll(),
		Opts: ExecutorOptions{Concurrency: 3},
	}
	ctx := WithActor(context.Background(), "tester")
	if _, err := exec.Execute(ctx, plan); err != nil {
		t.Fatal(err)
	}
	if max.Load() > 3 {
		t.Fatalf("concurrency exceeded: max=%d", max.Load())
	}
}

type trackingDeleter struct {
	onDelete func()
}

func (d *trackingDeleter) Delete(ctx context.Context, id ObjectID) error {
	d.onDelete()
	return nil
}

func TestExecuteRetryIdempotent(t *testing.T) {
	plan, deleter := newTestPlan(t, 3, 3)
	// obj-001 前两次删除失败，第三次成功；重试不得重复计数。
	var attempts atomic.Int32
	deleter.FailOn = map[ObjectID]error{}
	flaky := &flakyDeleter{inner: deleter, failTimes: 2, target: "obj-001", attempts: &attempts}
	exec := &Executor{
		Deleter: flaky, Store: NewMemoryCheckpointStore(), Auth: authAll(),
		Opts: ExecutorOptions{Concurrency: 1, MaxRetries: 3},
	}
	ctx := WithActor(context.Background(), "tester")
	res, err := exec.Execute(ctx, plan)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Deleted) != 3 {
		t.Fatalf("deleted=%d want 3, failed=%v", len(res.Deleted), res.Failed)
	}
	if attempts.Load() != 3 {
		t.Fatalf("attempts=%d want 3", attempts.Load())
	}
	// 幂等：obj-001 在结果中只出现一次。
	count := 0
	for _, id := range res.Deleted {
		if id == "obj-001" {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("obj-001 counted %d times", count)
	}
}

type flakyDeleter struct {
	inner     *MemoryDeleter
	failTimes int
	target    ObjectID
	attempts  *atomic.Int32
	mu        sync.Mutex
}

func (f *flakyDeleter) Delete(ctx context.Context, id ObjectID) error {
	if id == f.target {
		n := f.attempts.Add(1)
		if int(n) <= f.failTimes {
			return errors.New("transient error")
		}
	}
	return f.inner.Delete(ctx, id)
}

func TestExecuteCancelLeavesNoPartialState(t *testing.T) {
	plan, deleter := newTestPlan(t, 10, 2)
	ctx, cancel := context.WithCancel(WithActor(context.Background(), "tester"))
	store := NewMemoryCheckpointStore()
	// 第一批完成后取消。
	blocking := &cancelAfterDeleter{inner: deleter, cancel: cancel, after: 2}
	exec := &Executor{
		Deleter: blocking, Store: store, Auth: authAll(),
		Opts: ExecutorOptions{Concurrency: 1},
	}
	_, err := exec.Execute(ctx, plan)
	if err == nil {
		t.Fatal("expect interruption error")
	}
	// 检查点中只应有已提交项，且与已删除集合一致。
	cp, err := store.Load(context.Background(), plan.ID)
	if err != nil {
		t.Fatal(err)
	}
	for id, st := range cp.States {
		if st != StateCommitted {
			t.Fatalf("unexpected state %s=%s", id, st)
		}
	}
	if len(cp.States) != len(deleter.Deleted) {
		t.Fatalf("committed=%d deleted=%d mismatch", len(cp.States), len(deleter.Deleted))
	}
}

type cancelAfterDeleter struct {
	inner  *MemoryDeleter
	cancel context.CancelFunc
	after  int
	mu     sync.Mutex
	n      int
}

func (c *cancelAfterDeleter) Delete(ctx context.Context, id ObjectID) error {
	err := c.inner.Delete(ctx, id)
	c.mu.Lock()
	c.n++
	if c.n >= c.after {
		c.cancel()
	}
	c.mu.Unlock()
	return err
}

func TestExecuteResumeFromCheckpoint(t *testing.T) {
	plan, deleter := newTestPlan(t, 6, 2)
	store := NewMemoryCheckpointStore()
	ctx := WithActor(context.Background(), "tester")

	// 第一次：模拟中断，只提交前 2 个。
	cp := &Checkpoint{PlanID: plan.ID, States: map[ObjectID]ItemState{
		plan.Items[0].ID: StateCommitted,
		plan.Items[1].ID: StateCommitted,
	}}
	if err := store.Save(ctx, cp); err != nil {
		t.Fatal(err)
	}

	// 第二次执行：应从断点继续，已提交的不重复删除。
	exec := &Executor{
		Deleter: deleter, Store: store, Auth: authAll(),
		Opts: ExecutorOptions{Concurrency: 2},
	}
	res, err := exec.Execute(ctx, plan)
	if err != nil {
		t.Fatal(err)
	}
	if res.Resumed != 2 {
		t.Fatalf("resumed=%d want 2", res.Resumed)
	}
	if len(res.Deleted) != 4 {
		t.Fatalf("deleted=%d want 4", len(res.Deleted))
	}
	// 已提交的两个对象不得被重复删除。
	for _, id := range deleter.Deleted {
		if id == plan.Items[0].ID || id == plan.Items[1].ID {
			t.Fatalf("object %s deleted twice across resume", id)
		}
	}
	// 最终检查点应全部提交。
	final, _ := store.Load(ctx, plan.ID)
	if len(final.States) != 6 {
		t.Fatalf("final committed=%d want 6", len(final.States))
	}
}

func TestExecuteUnauthorized(t *testing.T) {
	plan, deleter := newTestPlan(t, 3, 3)
	auditor := &MemoryAuditor{}
	exec := &Executor{
		Deleter: deleter, Store: NewMemoryCheckpointStore(), Auditor: auditor,
		Auth: &StaticAuthorizer{Allowed: map[string]bool{"admin": true}},
		Opts: ExecutorOptions{Concurrency: 1},
	}
	ctx := WithActor(context.Background(), "intruder")
	_, err := exec.Execute(ctx, plan)
	if !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("want unauthorized error, got %v", err)
	}
	if len(deleter.Deleted) != 0 {
		t.Fatal("unauthorized request must not delete anything")
	}
	// 拒绝必须可追溯。
	var denied bool
	for _, r := range auditor.All() {
		if r.Action == ActionDenied && r.Actor == "intruder" {
			denied = true
		}
	}
	if !denied {
		t.Fatal("denied attempt must be audited")
	}
}
