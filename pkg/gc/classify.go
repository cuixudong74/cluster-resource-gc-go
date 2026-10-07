package gc

import (
	"fmt"
	"sort"
	"time"
)

// ClassifyOptions 分类参数。
type ClassifyOptions struct {
	// IdleThreshold 超过该时长未被使用且不可达的对象标记为空闲（idle）。
	// 为 0 时不做 idle 细分。
	IdleThreshold time.Duration
	// Now 判定用当前时间，便于测试注入。
	Now time.Time
}

// Classify 对快照中的每个对象给出判定结果。
// 流程：可达性分析 -> 不可达对象按原因细分（悬空引用 / 仅被垃圾引用 / 空闲 / 普通不可达）。
// 返回按 ID 排序的结果，保证确定性。
func Classify(snap *Snapshot, roots RootSet, opts ClassifyOptions) []Classification {
	g := BuildGraph(snap)

	var rootIDs []ObjectID
	rootIDs = append(rootIDs, roots.RootIDs...)
	rootKindSet := map[string]bool{}
	for _, k := range roots.RootKinds {
		rootKindSet[k] = true
	}
	for _, r := range snap.Resources {
		if rootKindSet[r.Kind] {
			rootIDs = append(rootIDs, r.ID)
		}
	}
	reachable := g.Reachable(rootIDs)

	// 第一遍：标出不可达对象（候选垃圾）。
	unreachable := map[ObjectID]bool{}
	for _, r := range snap.Resources {
		if !reachable[r.ID] {
			unreachable[r.ID] = true
		}
	}

	out := make([]Classification, 0, len(snap.Resources))
	for _, r := range snap.Resources {
		c := Classification{ID: r.ID}
		if !unreachable[r.ID] {
			// 可达对象：仍可能携带悬空引用，仅作提示，不判为垃圾。
			if dang := danglingRefs(g, &r); len(dang) > 0 {
				c.Detail = fmt.Sprintf("reachable but has dangling refs: %v", dang)
			}
			out = append(out, c)
			continue
		}
		c.Garbage = true
		c.Reason, c.Detail = reasonFor(g, &r, unreachable, opts)
		c.Category = categoryOf(c.Reason)
		out = append(out, c)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// danglingRefs 返回对象引用中不存在的目标。
func danglingRefs(g *Graph, r *Resource) []ObjectID {
	var out []ObjectID
	for _, ref := range r.Refs {
		if g.Get(ref) == nil {
			out = append(out, ref)
		}
	}
	if r.Owner != "" && g.Get(r.Owner) == nil {
		out = append(out, r.Owner)
	}
	return out
}

// reasonFor 为不可达对象选择最具体的判定原因。
func reasonFor(g *Graph, r *Resource, unreachable map[ObjectID]bool, opts ClassifyOptions) (GarbageReason, string) {
	// 1. 悬空引用：对象自身引用了不存在的目标。
	if dang := danglingRefs(g, r); len(dang) > 0 {
		return ReasonDanglingRef, fmt.Sprintf("references missing objects: %v", dang)
	}
	// 2. 仅被垃圾引用的中间对象：所有引用者都已是垃圾。
	refs := g.Referrers(r.ID)
	if len(refs) > 0 {
		allGarbage := true
		for _, ref := range refs {
			if !unreachable[ref] {
				allGarbage = false
				break
			}
		}
		if allGarbage {
			return ReasonGarbageOwned, fmt.Sprintf("only referenced by garbage objects: %v", refs)
		}
	}
	// 3. 长期未使用的空闲对象。
	if opts.IdleThreshold > 0 && !r.LastUsed.IsZero() &&
		opts.Now.Sub(r.LastUsed) > opts.IdleThreshold {
		return ReasonIdle, fmt.Sprintf("unused for %s (threshold %s)",
			opts.Now.Sub(r.LastUsed).Round(time.Second), opts.IdleThreshold)
	}
	// 4. 普通不可达。
	return ReasonUnreachable, "not reachable from any live root"
}

// categoryOf 将原因映射为回收类别：悬空引用需人工确认，其余可自动回收。
func categoryOf(reason GarbageReason) Category {
	switch reason {
	case ReasonDanglingRef:
		return CategoryManual
	default:
		return CategoryAuto
	}
}
