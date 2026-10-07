package gc

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

// ExecutorOptions 执行器参数。
type ExecutorOptions struct {
	// Concurrency 单批内最大并发删除数，<=0 视为 1。
	Concurrency int
	// RatePerSecond 每秒最多发起的删除数，<=0 表示不限速。
	RatePerSecond float64
	// MaxRetries 单个对象失败后的最大重试次数（重试幂等）。
	MaxRetries int
	// RetryBackoff 重试间隔。
	RetryBackoff time.Duration
	// Now 时间源，便于测试注入。
	Now func() time.Time
}

func (o *ExecutorOptions) withDefaults() ExecutorOptions {
	out := *o
	if out.Concurrency <= 0 {
		out.Concurrency = 1
	}
	if out.MaxRetries < 0 {
		out.MaxRetries = 0
	}
	if out.Now == nil {
		out.Now = time.Now
	}
	return out
}

// ExecResult 执行结果汇总。
type ExecResult struct {
	PlanID  string              `json:"plan_id"`
	Deleted []ObjectID          `json:"deleted"`
	Failed  map[ObjectID]string `json:"failed,omitempty"`
	// Resumed 本次因检查点已提交而跳过的数量（中断恢复）。
	Resumed int `json:"resumed"`
}

// Executor 按计划执行回收：分批、并发、限速、幂等重试、断点续跑。
type Executor struct {
	Deleter Deleter
	Store   CheckpointStore
	Auditor Auditor
	Auth    Authorizer
	Opts    ExecutorOptions
}

// Execute 执行回收计划。支持 ctx 取消/超时：正在进行的删除会等待其返回，
// 未开始的批次不再启动；已提交进度落盘，重启后可从断点继续。
func (e *Executor) Execute(ctx context.Context, plan *Plan) (*ExecResult, error) {
	if plan == nil {
		return nil, fmt.Errorf("nil plan")
	}
	opts := e.Opts.withDefaults()
	actor := ActorFrom(ctx)

	// 越权拦截：未授权直接拒绝并留痕。
	if err := checkAuth(e.Auditor, e.Auth, actor, ActionDelete, plan.ID, "", opts.Now()); err != nil {
		return nil, err
	}

	// 加载断点。
	cp, err := e.loadCheckpoint(ctx, plan.ID)
	if err != nil {
		return nil, err
	}

	res := &ExecResult{PlanID: plan.ID, Failed: map[ObjectID]string{}}
	itemByID := make(map[ObjectID]PlanItem, len(plan.Items))
	for _, it := range plan.Items {
		itemByID[it.ID] = it
	}

	// 限速器。
	var ticker *time.Ticker
	if opts.RatePerSecond > 0 {
		interval := time.Duration(float64(time.Second) / opts.RatePerSecond)
		ticker = time.NewTicker(interval)
		defer ticker.Stop()
	}

	for _, batch := range plan.Batches {
		// 批开始前检查取消：不启动新批次。
		if err := ctx.Err(); err != nil {
			return res, fmt.Errorf("execute interrupted: %w", err)
		}
		// 过滤已提交项（断点续跑：不重复执行）。
		var todo []ObjectID
		for _, id := range batch {
			switch cp.States[id] {
			case StateCommitted:
				// 已提交：跳过，不重复应用。
				res.Resumed++
			default:
				// pending 或 failed：删除幂等，重新执行不会重复计数。
				todo = append(todo, id)
			}
		}
		if len(todo) == 0 {
			continue
		}
		if err := e.runBatch(ctx, plan, todo, itemByID, cp, res, opts, ticker, actor); err != nil {
			return res, err
		}
	}
	sort.Slice(res.Deleted, func(i, j int) bool { return res.Deleted[i] < res.Deleted[j] })
	if e.Auditor != nil {
		e.Auditor.Record(AuditRecord{
			At: opts.Now(), Actor: actor, Action: ActionComplete, PlanID: plan.ID,
			Detail: fmt.Sprintf("deleted=%d failed=%d resumed=%d", len(res.Deleted), len(res.Failed), res.Resumed),
		})
	}
	return res, nil
}

// runBatch 并发执行单批删除，受并发度与速率限制。
func (e *Executor) runBatch(ctx context.Context, plan *Plan, ids []ObjectID,
	itemByID map[ObjectID]PlanItem, cp *Checkpoint, res *ExecResult,
	opts ExecutorOptions, ticker *time.Ticker, actor string) error {

	sem := make(chan struct{}, opts.Concurrency)
	var wg sync.WaitGroup
	var mu sync.Mutex

	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			wg.Wait()
			return fmt.Errorf("batch interrupted: %w", err)
		}
		if ticker != nil {
			select {
			case <-ctx.Done():
				wg.Wait()
				return fmt.Errorf("batch interrupted: %w", ctx.Err())
			case <-ticker.C:
			}
		}
		sem <- struct{}{}
		wg.Add(1)
		go func(id ObjectID) {
			defer wg.Done()
			defer func() { <-sem }()
			err := e.deleteWithRetry(ctx, id, opts)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					// 取消/超时：不提交状态，保持 pending，下次从断点继续。
					return
				}
				cp.States[id] = StateFailed
				res.Failed[id] = err.Error()
			} else {
				cp.States[id] = StateCommitted
				res.Deleted = append(res.Deleted, id)
			}
			cp.UpdatedAt = opts.Now()
			// 每项完成后立即落盘，保证崩溃可恢复。
			// 用 WithoutCancel：即使外层已取消，已完成的删除也要能提交落盘。
			if saveErr := e.Store.Save(context.WithoutCancel(ctx), cp); saveErr != nil {
				res.Failed[id] = fmt.Sprintf("checkpoint save: %v", saveErr)
			}
			if e.Auditor != nil {
				action := ActionDelete
				detail := "ok"
				if err != nil {
					detail = err.Error()
				}
				e.Auditor.Record(AuditRecord{
					At: opts.Now(), Actor: actor, Action: action,
					ObjectID: id, PlanID: plan.ID, Detail: detail,
				})
			}
		}(id)
	}
	wg.Wait()
	return nil
}

// deleteWithRetry 幂等重试删除单个对象。
func (e *Executor) deleteWithRetry(ctx context.Context, id ObjectID, opts ExecutorOptions) error {
	var lastErr error
	for attempt := 0; attempt <= opts.MaxRetries; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := e.Deleter.Delete(ctx, id); err != nil {
			lastErr = err
			if opts.RetryBackoff > 0 {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(opts.RetryBackoff):
				}
			}
			continue
		}
		return nil
	}
	return lastErr
}

// loadCheckpoint 加载或初始化检查点。
func (e *Executor) loadCheckpoint(ctx context.Context, planID string) (*Checkpoint, error) {
	if e.Store == nil {
		return &Checkpoint{PlanID: planID, States: map[ObjectID]ItemState{}}, nil
	}
	cp, err := e.Store.Load(ctx, planID)
	if err != nil {
		return nil, err
	}
	if cp == nil {
		cp = &Checkpoint{PlanID: planID, States: map[ObjectID]ItemState{}}
	}
	if cp.States == nil {
		cp.States = map[ObjectID]ItemState{}
	}
	return cp, nil
}
