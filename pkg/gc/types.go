// Package gc 提供集群遗留资源的识别与安全回收能力。
package gc

import "time"

// ObjectID 唯一标识一个集群对象。
type ObjectID string

// Resource 表示集群中的一个对象及其归属与引用关系。
type Resource struct {
	ID        ObjectID          `json:"id"`
	Kind      string            `json:"kind"`
	Namespace string            `json:"namespace"`
	Name      string            `json:"name"`
	Owner     ObjectID          `json:"owner,omitempty"` // 归属对象，空表示无属主
	Refs      []ObjectID        `json:"refs,omitempty"`  // 该对象引用的其他对象
	Labels    map[string]string `json:"labels,omitempty"`
	CreatedAt time.Time         `json:"created_at"`
	LastUsed  time.Time         `json:"last_used"` // 最近一次被使用的时间
}

// Snapshot 是一份集群对象的完整快照。
type Snapshot struct {
	TakenAt   time.Time  `json:"taken_at"`
	Resources []Resource `json:"resources"`
}

// RootSet 描述存活根对象集合（从哪些对象出发做可达性分析）。
type RootSet struct {
	// RootKinds 这些 Kind 的对象一律视为存活根。
	RootKinds []string `json:"root_kinds,omitempty"`
	// RootIDs 显式指定的根对象。
	RootIDs []ObjectID `json:"root_ids,omitempty"`
}

// GarbageReason 区分遗留资源的判定原因。
type GarbageReason string

const (
	// ReasonUnreachable 从存活根不可达。
	ReasonUnreachable GarbageReason = "unreachable"
	// ReasonDanglingRef 对象引用了不存在的对象（悬空引用）。
	ReasonDanglingRef GarbageReason = "dangling-reference"
	// ReasonGarbageOwned 仅被垃圾对象引用的中间对象。
	ReasonGarbageOwned GarbageReason = "garbage-only-referenced"
	// ReasonIdle 长期未使用的空闲对象。
	ReasonIdle GarbageReason = "idle"
)

// Category 回收类别。
type Category string

const (
	// CategoryAuto 可直接自动回收。
	CategoryAuto Category = "auto"
	// CategoryManual 需要人工确认后才能回收。
	CategoryManual Category = "manual"
)

// Classification 是单个对象的判定结果。
type Classification struct {
	ID       ObjectID      `json:"id"`
	Garbage  bool          `json:"garbage"`
	Reason   GarbageReason `json:"reason,omitempty"`
	Category Category      `json:"category,omitempty"`
	Detail   string        `json:"detail,omitempty"` // 可解释的判定说明
}

// PlanItem 计划中的单个待回收对象。
type PlanItem struct {
	ID       ObjectID      `json:"id"`
	Reason   GarbageReason `json:"reason"`
	Category Category      `json:"category"`
	Detail   string        `json:"detail,omitempty"`
}

// SkippedItem 被保护策略或人工确认门槛拦截的对象。
type SkippedItem struct {
	ID     ObjectID `json:"id"`
	Reason string   `json:"reason"`
}
