package gc

import (
	"encoding/json"
	"strings"
	"time"
)

const (
	ReasonExplicitlyProtected = "explicitly-protected"
	ReasonProtectedLabel      = "protected-label"
	ReasonProtectedAnnotation = "protected-annotation"
	ReasonGracePeriod         = "within-grace-period"
)

// PolicyDuration is a time.Duration that can be configured as "24h" in JSON.
type PolicyDuration time.Duration

// Duration returns the standard-library duration.
func (d PolicyDuration) Duration() time.Duration { return time.Duration(d) }

// UnmarshalJSON accepts a duration string or numeric nanosecond value.
func (d *PolicyDuration) UnmarshalJSON(b []byte) error {
	var text string
	if err := json.Unmarshal(b, &text); err == nil {
		parsed, err := time.ParseDuration(text)
		if err != nil {
			return err
		}
		*d = PolicyDuration(parsed)
		return nil
	}
	var nanos int64
	if err := json.Unmarshal(b, &nanos); err != nil {
		return err
	}
	*d = PolicyDuration(nanos)
	return nil
}

// MarshalJSON renders the duration as a human-readable string.
func (d PolicyDuration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(d).String())
}

// Selector matches resources by stable metadata.
type Selector struct {
	Kinds      []string          `json:"kinds,omitempty"`
	Namespaces []string          `json:"namespaces,omitempty"`
	Labels     map[string]string `json:"labels,omitempty"`
}

// ProtectionRule is an explicit, explainable protection rule.
type ProtectionRule struct {
	ID          string   `json:"id"`
	Description string   `json:"description,omitempty"`
	Selector    Selector `json:"selector"`
	// MinAge only protects resources younger than this age. It is useful for
	// rules such as "protect ConfigMaps younger than one hour."
	MinAge PolicyDuration `json:"minAge,omitempty"`
}

// Policy contains all non-deletable conditions.
type Policy struct {
	Now                 time.Time        `json:"-"`
	GracePeriod         PolicyDuration   `json:"gracePeriod,omitempty"`
	IdleThreshold       PolicyDuration   `json:"idleThreshold,omitempty"`
	ProtectedIDs        []ObjectID       `json:"protectedIDs,omitempty"`
	Rules               []ProtectionRule `json:"rules,omitempty"`
	ProtectedLabel      string           `json:"protectedLabel,omitempty"`
	ProtectedAnnotation string           `json:"protectedAnnotation,omitempty"`
}

// ProtectionDecision is an explicit protection explanation.
type ProtectionDecision struct {
	Protected bool
	Reasons   []string
}

// Protect evaluates a resource against marked, grace, and rule protections.
// Protected resources cannot be deleted even when classified as garbage.
func (p Policy) Protect(obj Object) ProtectionDecision {
	decision := ProtectionDecision{Reasons: []string{}}
	for _, id := range p.ProtectedIDs {
		if id == obj.ID {
			decision.Protected = true
			decision.Reasons = append(decision.Reasons, ReasonExplicitlyProtected)
			break
		}
	}

	if p.ProtectedLabel != "" {
		if value, ok := obj.Labels[p.ProtectedLabel]; ok && isTruthy(value) {
			decision.Protected = true
			decision.Reasons = append(decision.Reasons, ReasonProtectedLabel)
		}
	}
	if p.ProtectedAnnotation != "" {
		if value, ok := obj.Annotations[p.ProtectedAnnotation]; ok && isTruthy(value) {
			decision.Protected = true
			decision.Reasons = append(decision.Reasons, ReasonProtectedAnnotation)
		}
	}

	now := p.Now
	if p.GracePeriod > 0 && !now.IsZero() && !obj.CreatedAt.IsZero() && now.Sub(obj.CreatedAt) < p.GracePeriod.Duration() {
		decision.Protected = true
		decision.Reasons = append(decision.Reasons, ReasonGracePeriod)
	}

	for _, rule := range p.Rules {
		if matchesSelector(obj, rule.Selector) {
			if rule.MinAge <= 0 || (!now.IsZero() && !obj.CreatedAt.IsZero() && now.Sub(obj.CreatedAt) < rule.MinAge.Duration()) {
				decision.Protected = true
				decision.Reasons = append(decision.Reasons, "protection-rule:"+rule.ID)
			}
		}
	}
	return decision
}

func matchesSelector(obj Object, selector Selector) bool {
	if len(selector.Kinds) > 0 && !containsString(selector.Kinds, obj.ID.Kind) {
		return false
	}
	if len(selector.Namespaces) > 0 && !containsString(selector.Namespaces, obj.ID.Namespace) {
		return false
	}
	for key, want := range selector.Labels {
		got, ok := obj.Labels[key]
		if !ok || got != want {
			return false
		}
	}
	return true
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func isTruthy(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "false", "0", "no", "off":
		return false
	default:
		return true
	}
}
