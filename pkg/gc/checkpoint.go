package gc

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// ItemState 单个计划项的回收状态。
type ItemState string

const (
	// StatePending 未开始。
	StatePending ItemState = "pending"
	// StateCommitted 已提交（删除成功并落盘）。
	StateCommitted ItemState = "committed"
	// StateFailed 已失败（达到重试上限）。
	StateFailed ItemState = "failed"
)

// Checkpoint 记录一次计划执行的进度，用于中断恢复。
type Checkpoint struct {
	PlanID string `json:"plan_id"`
	// States 已提交与失败的状态；未出现的 ID 视为 pending。
	States map[ObjectID]ItemState `json:"states"`
	// UpdatedAt 最近一次提交时间。
	UpdatedAt time.Time `json:"updated_at"`
}

// Pending 返回尚未提交也未失败的计划项。
func (c *Checkpoint) Pending(items []PlanItem) []PlanItem {
	var out []PlanItem
	for _, it := range items {
		if c.States[it.ID] == "" {
			out = append(out, it)
		}
	}
	return out
}

// CheckpointStore 检查点持久化抽象。
type CheckpointStore interface {
	// Load 读取检查点，不存在返回 (nil, nil)。
	Load(ctx context.Context, planID string) (*Checkpoint, error)
	// Save 原子保存检查点。
	Save(ctx context.Context, cp *Checkpoint) error
}

// FileCheckpointStore 基于本地文件的检查点存储（写临时文件 + rename 保证原子性）。
type FileCheckpointStore struct {
	Dir string
}

func (f *FileCheckpointStore) path(planID string) string {
	return filepath.Join(f.Dir, "gc-checkpoint-"+planID+".json")
}

// Load 实现 CheckpointStore。
func (f *FileCheckpointStore) Load(ctx context.Context, planID string) (*Checkpoint, error) {
	data, err := os.ReadFile(f.path(planID))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read checkpoint: %w", err)
	}
	var cp Checkpoint
	if err := json.Unmarshal(data, &cp); err != nil {
		return nil, fmt.Errorf("parse checkpoint: %w", err)
	}
	return &cp, nil
}

// Save 实现 CheckpointStore，原子写入。
func (f *FileCheckpointStore) Save(ctx context.Context, cp *Checkpoint) error {
	if err := os.MkdirAll(f.Dir, 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(cp, "", "  ")
	if err != nil {
		return err
	}
	tmp := f.path(cp.PlanID) + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return fmt.Errorf("write checkpoint: %w", err)
	}
	if err := os.Rename(tmp, f.path(cp.PlanID)); err != nil {
		return fmt.Errorf("commit checkpoint: %w", err)
	}
	return nil
}

// MemoryCheckpointStore 内存检查点存储，供测试。
type MemoryCheckpointStore struct {
	mu   sync.Mutex
	data map[string]*Checkpoint
}

// NewMemoryCheckpointStore 创建内存检查点存储。
func NewMemoryCheckpointStore() *MemoryCheckpointStore {
	return &MemoryCheckpointStore{data: map[string]*Checkpoint{}}
}

// Load 实现 CheckpointStore。
func (m *MemoryCheckpointStore) Load(ctx context.Context, planID string) (*Checkpoint, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := m.data[planID]
	if cp == nil {
		return nil, nil
	}
	// 返回副本，避免外部修改。
	dup := *cp
	states := make(map[ObjectID]ItemState, len(cp.States))
	for k, v := range cp.States {
		states[k] = v
	}
	dup.States = states
	return &dup, nil
}

// Save 实现 CheckpointStore。
func (m *MemoryCheckpointStore) Save(ctx context.Context, cp *Checkpoint) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	dup := *cp
	states := make(map[ObjectID]ItemState, len(cp.States))
	for k, v := range cp.States {
		states[k] = v
	}
	dup.States = states
	m.data[cp.PlanID] = &dup
	return nil
}
