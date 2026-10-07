package gc

import (
	"context"
	"testing"
	"time"
)

func TestExecuteRateLimit(t *testing.T) {
	plan, deleter := newTestPlan(t, 5, 5)
	exec := &Executor{
		Deleter: deleter, Store: NewMemoryCheckpointStore(), Auth: authAll(),
		Opts: ExecutorOptions{Concurrency: 5, RatePerSecond: 20}, // 50ms/个
	}
	ctx := WithActor(context.Background(), "tester")
	start := time.Now()
	res, err := exec.Execute(ctx, plan)
	if err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(start)
	if len(res.Deleted) != 5 {
		t.Fatalf("deleted=%d want 5", len(res.Deleted))
	}
	// 5 个对象、20/s：至少间隔 4 个令牌 ≈ 200ms，允许一定调度误差。
	if elapsed < 150*time.Millisecond {
		t.Fatalf("rate limit not enforced: 5 deletes in %s", elapsed)
	}
}
