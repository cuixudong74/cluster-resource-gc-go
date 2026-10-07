package gc

import (
	"fmt"
	"time"
)

// ProtectRule 一条可配置的保护规则：按 Kind / Namespace / 标签匹配。
// 所有非空字段都匹配（AND）时规则命中。
type ProtectRule struct {
	Name      string            `json:"name"`
	Kind      string            `json:"kind,omitempty"`
	Namespace string            `json:"namespace,omitempty"`
	Labels    map[string]string `json:"labels,omitempty"`
}

// Matches 判断规则是否命中对象。
func (p *ProtectRule) Matches(r *Resource) bool {
	if p.Kind != "" && p.Kind != r.Kind {
		return false
	}
	if p.Namespace != "" && p.Namespace != r.Namespace {
		return false
	}
	for k, v := range p.Labels {
		if r.Labels[k] != v {
			return false
		}
	}
	return true
}

// ProtectPolicy 保护策略集合。
type ProtectPolicy struct {
	// ProtectedLabel 携带该标签（值为 "true"）的对象一律保护，空表示禁用。
	ProtectedLabel string `json:"protected_label,omitempty"`
	// GracePeriod 创建时间距今不足该时长的对象处于宽限期，受保护。
	GracePeriod time.Duration `json:"grace_period,omitempty"`
	// Rules 自定义保护规则。
	Rules []ProtectRule `json:"rules,omitempty"`
	// Now 判定用当前时间，便于测试注入。
	Now time.Time `json:"-"`
}

// Protection 判定结果，携带可解释的原因。
type Protection struct {
	Protected bool   `json:"protected"`
	Reason    string `json:"reason,omitempty"`
}

// Check 判定对象是否受保护，返回可解释的结果。
func (p *ProtectPolicy) Check(r *Resource) Protection {
	if p == nil {
		return Protection{}
	}
	now := p.Now
	if now.IsZero() {
		now = time.Now()
	}
	if p.ProtectedLabel != "" && r.Labels[p.ProtectedLabel] == "true" {
		return Protection{Protected: true,
			Reason: fmt.Sprintf("label %q=true", p.ProtectedLabel)}
	}
	if p.GracePeriod > 0 && !r.CreatedAt.IsZero() &&
		now.Sub(r.CreatedAt) < p.GracePeriod {
		return Protection{Protected: true,
			Reason: fmt.Sprintf("within grace period: age %s < %s",
				now.Sub(r.CreatedAt).Round(time.Second), p.GracePeriod)}
	}
	for _, rule := range p.Rules {
		if rule.Matches(r) {
			return Protection{Protected: true,
				Reason: fmt.Sprintf("matched protect rule %q", rule.Name)}
		}
	}
	return Protection{}
}
