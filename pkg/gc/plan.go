package gc

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"time"
)

// PlanOptions 生成回收计划的参数。
type PlanOptions struct {
	// BatchSize 每批最大对象数，<=0 表示不分批（全部一批）。
	BatchSize int
	// IncludeManual 是否把需人工确认的对象也纳入计划（默认只计划可自动回收的）。
	IncludeManual bool
	// Policy 保护策略，命中者从计划中剔除并记录原因。
	Policy *ProtectPolicy
	// Classify 分类参数。
	Classify ClassifyOptions
}

// Plan 是一份确定性的干跑回收计划。
type Plan struct {
	// ID 计划指纹：同一份输入多次生成结果完全一致。
	ID string `json:"id"`
	// CreatedAt 计划生成时间。
	CreatedAt time.Time `json:"created_at"`
	// Items 待回收对象，按 ID 排序，确定性。
	Items []PlanItem `json:"items"`
	// Skipped 被保护策略拦截的对象及原因（可解释）。
	Skipped []SkippedItem `json:"skipped,omitempty"`
	// Batches 分批视图：Items 按 BatchSize 切分后的批次（每批内 ID 有序）。
	Batches [][]ObjectID `json:"batches"`
	// Summary 汇总统计。
	Summary PlanSummary `json:"summary"`
}

// PlanSummary 计划汇总。
type PlanSummary struct {
	Total      int            `json:"total"`
	ByReason   map[string]int `json:"by_reason"`
	ByCategory map[string]int `json:"by_category"`
	Skipped    int            `json:"skipped"`
	BatchCount int            `json:"batch_count"`
}

// BuildPlan 根据快照与根集合生成确定性回收计划（干跑）。
// 相同输入（快照、根、选项）多次调用产生完全一致的 Plan.ID 与内容。
func BuildPlan(snap *Snapshot, roots RootSet, opts PlanOptions) (*Plan, error) {
	if snap == nil {
		return nil, fmt.Errorf("nil snapshot")
	}
	classes := Classify(snap, roots, opts.Classify)
	byID := make(map[ObjectID]*Resource, len(snap.Resources))
	for i := range snap.Resources {
		byID[snap.Resources[i].ID] = &snap.Resources[i]
	}

	plan := &Plan{
		CreatedAt: opts.Classify.Now,
		Summary: PlanSummary{
			ByReason:   map[string]int{},
			ByCategory: map[string]int{},
		},
	}
	for _, c := range classes {
		if !c.Garbage {
			continue
		}
		r := byID[c.ID]
		// 保护策略在任何情况下优先：命中即跳过并记录原因。
		if prot := opts.Policy.Check(r); prot.Protected {
			plan.Skipped = append(plan.Skipped, SkippedItem{ID: c.ID, Reason: prot.Reason})
			continue
		}
		if c.Category == CategoryManual && !opts.IncludeManual {
			plan.Skipped = append(plan.Skipped, SkippedItem{ID: c.ID,
				Reason: "requires manual confirmation"})
			continue
		}
		plan.Items = append(plan.Items, PlanItem{
			ID: c.ID, Reason: c.Reason, Category: c.Category, Detail: c.Detail,
		})
	}
	// 排序保证确定性（Classify 已按 ID 排序，这里再保险一次）。
	sort.Slice(plan.Items, func(i, j int) bool { return plan.Items[i].ID < plan.Items[j].ID })
	sort.Slice(plan.Skipped, func(i, j int) bool { return plan.Skipped[i].ID < plan.Skipped[j].ID })

	// 汇总。
	for _, it := range plan.Items {
		plan.Summary.ByReason[string(it.Reason)]++
		plan.Summary.ByCategory[string(it.Category)]++
	}
	plan.Summary.Total = len(plan.Items)
	plan.Summary.Skipped = len(plan.Skipped)

	// 分批。
	plan.Batches = splitBatches(plan.Items, opts.BatchSize)
	plan.Summary.BatchCount = len(plan.Batches)

	// 计划指纹：对规范化内容做哈希，保证同输入同 ID。
	plan.ID = planFingerprint(plan)
	return plan, nil
}

// splitBatches 将计划项按批大小切分，每批内保持 ID 有序。
func splitBatches(items []PlanItem, batchSize int) [][]ObjectID {
	if batchSize <= 0 {
		batchSize = len(items)
		if batchSize == 0 {
			return nil
		}
	}
	var batches [][]ObjectID
	for i := 0; i < len(items); i += batchSize {
		end := i + batchSize
		if end > len(items) {
			end = len(items)
		}
		batch := make([]ObjectID, 0, end-i)
		for _, it := range items[i:end] {
			batch = append(batch, it.ID)
		}
		batches = append(batches, batch)
	}
	return batches
}

// planFingerprint 计算计划内容指纹（不含易变字段 CreatedAt）。
func planFingerprint(p *Plan) string {
	type canon struct {
		Items   []PlanItem    `json:"items"`
		Skipped []SkippedItem `json:"skipped"`
	}
	data, _ := json.Marshal(canon{Items: p.Items, Skipped: p.Skipped})
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:16])
}
