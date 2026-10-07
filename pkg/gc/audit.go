package gc

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Action 审计动作类型。
type Action string

const (
	ActionPlan     Action = "plan"
	ActionDelete   Action = "delete"
	ActionDenied   Action = "denied"
	ActionResume   Action = "resume"
	ActionComplete Action = "complete"
)

// AuditRecord 一次回收相关动作的审计记录。
type AuditRecord struct {
	At       time.Time `json:"at"`
	Actor    string    `json:"actor"`
	Action   Action    `json:"action"`
	ObjectID ObjectID  `json:"object_id,omitempty"`
	PlanID   string    `json:"plan_id,omitempty"`
	Detail   string    `json:"detail,omitempty"`
}

// Auditor 审计记录落点抽象。
type Auditor interface {
	Record(rec AuditRecord)
}

// MemoryAuditor 内存审计器，供测试与本地运行。
type MemoryAuditor struct {
	mu      sync.Mutex
	Records []AuditRecord
}

// Record 实现 Auditor。
func (m *MemoryAuditor) Record(rec AuditRecord) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Records = append(m.Records, rec)
}

// All 返回全部审计记录副本。
func (m *MemoryAuditor) All() []AuditRecord {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]AuditRecord(nil), m.Records...)
}

// Authorizer 越权拦截抽象：判定某个操作者是否允许执行回收。
type Authorizer interface {
	// Authorize 返回 nil 表示允许，否则返回拒绝原因。
	Authorize(actor string, action Action, id ObjectID) error
}

// StaticAuthorizer 基于允许列表的简单授权器。
// Allowed 为空表示拒绝一切（安全默认）；"*" 表示放行全部。
type StaticAuthorizer struct {
	Allowed map[string]bool
}

// Authorize 实现 Authorizer。
func (s *StaticAuthorizer) Authorize(actor string, action Action, id ObjectID) error {
	if s.Allowed["*"] || s.Allowed[actor] {
		return nil
	}
	return fmt.Errorf("actor %q is not authorized for %s", actor, action)
}

// ErrUnauthorized 未授权错误标记。
var ErrUnauthorized = fmt.Errorf("unauthorized")

// checkAuth 统一鉴权入口：拒绝时写审计并返回错误。
func checkAuth(aud Auditor, auth Authorizer, actor string, action Action, planID string, id ObjectID, now time.Time) error {
	if auth == nil {
		return fmt.Errorf("%w: no authorizer configured", ErrUnauthorized)
	}
	if err := auth.Authorize(actor, action, id); err != nil {
		if aud != nil {
			aud.Record(AuditRecord{
				At: now, Actor: actor, Action: ActionDenied,
				ObjectID: id, PlanID: planID, Detail: err.Error(),
			})
		}
		return fmt.Errorf("%w: %v", ErrUnauthorized, err)
	}
	return nil
}

// contextKey 用于从 context 传递操作者。
type contextKey string

const actorKey contextKey = "gc-actor"

// WithActor 将操作者放入 context。
func WithActor(ctx context.Context, actor string) context.Context {
	return context.WithValue(ctx, actorKey, actor)
}

// ActorFrom 从 context 取操作者，缺省为 "anonymous"。
func ActorFrom(ctx context.Context) string {
	if v, ok := ctx.Value(actorKey).(string); ok && v != "" {
		return v
	}
	return "anonymous"
}
