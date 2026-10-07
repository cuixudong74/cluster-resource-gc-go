package gc

import (
	"context"
	"fmt"
	"time"
)

// Config 回收器整体配置。
type Config struct {
	Roots    RootSet         `json:"roots"`
	Policy   ProtectPolicy   `json:"policy"`
	Plan     PlanOptions     `json:"plan"`
	Executor ExecutorOptions `json:"executor"`
}

// GC 编排器：加载快照 -> 分类 -> 保护过滤 -> 干跑计划 -> 执行。
type GC struct {
	Source  Source
	Deleter Deleter
	Store   CheckpointStore
	Auditor Auditor
	Auth    Authorizer
	Config  Config
}

// DryRun 只生成回收计划，不执行任何删除。
func (g *GC) DryRun(ctx context.Context) (*Plan, error) {
	snap, err := g.Source.Load(ctx)
	if err != nil {
		return nil, fmt.Errorf("load snapshot: %w", err)
	}
	opts := g.Config.Plan
	opts.Policy = &g.Config.Policy
	plan, err := BuildPlan(snap, g.Config.Roots, opts)
	if err != nil {
		return nil, err
	}
	if g.Auditor != nil {
		g.Auditor.Record(AuditRecord{
			At: nowOf(g.Config.Executor), Actor: ActorFrom(ctx), Action: ActionPlan,
			PlanID: plan.ID,
			Detail: fmt.Sprintf("items=%d skipped=%d batches=%d",
				plan.Summary.Total, plan.Summary.Skipped, plan.Summary.BatchCount),
		})
	}
	return plan, nil
}

// Run 先生成干跑计划，再按计划执行。实际删除严格对齐计划内容。
func (g *GC) Run(ctx context.Context) (*Plan, *ExecResult, error) {
	plan, err := g.DryRun(ctx)
	if err != nil {
		return nil, nil, err
	}
	exec := &Executor{
		Deleter: g.Deleter,
		Store:   g.Store,
		Auditor: g.Auditor,
		Auth:    g.Auth,
		Opts:    g.Config.Executor,
	}
	res, err := exec.Execute(ctx, plan)
	if err != nil {
		return plan, res, err
	}
	return plan, res, nil
}

func nowOf(opts ExecutorOptions) time.Time {
	if opts.Now != nil {
		return opts.Now()
	}
	return time.Now()
}
