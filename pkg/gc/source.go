package gc

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"sync"
)

// Source 资源来源抽象：可替换为本地快照、模拟数据源或真实集群客户端。
type Source interface {
	// Load 读取一份完整快照。
	Load(ctx context.Context) (*Snapshot, error)
}

// Deleter 回收动作的落点抽象：真实实现会调用集群 API，测试可用内存实现。
type Deleter interface {
	// Delete 删除指定对象，必须幂等：删除不存在的对象返回 nil。
	Delete(ctx context.Context, id ObjectID) error
}

// FileSource 从本地 JSON 快照文件读取，不依赖真实集群与网络。
type FileSource struct {
	Path string
}

// Load 实现 Source。
func (f *FileSource) Load(ctx context.Context) (*Snapshot, error) {
	data, err := os.ReadFile(f.Path)
	if err != nil {
		return nil, fmt.Errorf("read snapshot file: %w", err)
	}
	var snap Snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return nil, fmt.Errorf("parse snapshot file: %w", err)
	}
	return &snap, nil
}

// MemorySource 内存模拟数据源，供本地运行与自动化测试使用。
type MemorySource struct {
	Snap Snapshot
}

// Load 实现 Source。
func (m *MemorySource) Load(ctx context.Context) (*Snapshot, error) {
	snap := m.Snap
	return &snap, nil
}

// MemoryDeleter 内存删除器，幂等且记录删除顺序，供测试与干跑验证。
type MemoryDeleter struct {
	mu      sync.Mutex
	present map[ObjectID]bool
	Deleted []ObjectID
	// FailOn 模拟失败：这些对象在删除时返回错误（用于重试测试）。
	FailOn map[ObjectID]error
}

// NewMemoryDeleter 以一组现存对象创建删除器。
func NewMemoryDeleter(existing []ObjectID) *MemoryDeleter {
	d := &MemoryDeleter{present: map[ObjectID]bool{}}
	for _, id := range existing {
		d.present[id] = true
	}
	return d
}

// Delete 实现 Deleter，幂等。
func (d *MemoryDeleter) Delete(ctx context.Context, id ObjectID) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err, ok := d.FailOn[id]; ok {
		return err
	}
	if d.present[id] {
		delete(d.present, id)
		d.Deleted = append(d.Deleted, id)
	}
	return nil // 不存在也视为成功，保证幂等
}

// DeletedSorted 返回排序后的已删除列表，便于断言。
func (d *MemoryDeleter) DeletedSorted() []ObjectID {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := append([]ObjectID(nil), d.Deleted...)
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}
