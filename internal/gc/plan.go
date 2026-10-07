package gc

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"time"
)

const (
	StrategyAuto   = "auto"
	StrategyManual = "manual"
	StrategyNone   = "none"
	ActionSkip     = "skip"
	ActionDelete   = "delete"
)

// PlanItem is one deterministic planned action or skip.
type PlanItem struct {
	ID             ObjectID `json:"id"`
	Action         string   `json:"action"`
	Strategy       string   `json:"strategy,omitempty"`
	Reasons        []string `json:"reasons,omitempty"`
	Protection     []string `json:"protection,omitempty"`
	IdempotencyKey string   `json:"idempotencyKey,omitempty"`
}

// Plan is generated before any mutation and is stable for identical input.
type Plan struct {
	PlanID             string              `json:"planId"`
	InputFingerprint   string              `json:"inputFingerprint"`
	CreatedAt          time.Time           `json:"createdAt"`
	Items              []PlanItem          `json:"items"`
	DanglingReferences []DanglingReference `json:"danglingReferences,omitempty"`
}

// Planner converts analysis into a deterministic dry-run plan.
type Planner struct {
	Policy Policy
	// ApprovedManual is the allow-list of manual items approved by a human.
	ApprovedManual map[ObjectID]bool
}

// DryRun creates a plan without performing deletion.
func (p *Planner) DryRun(snapshot *Snapshot, analysis *Analysis) (*Plan, error) {
	if snapshot == nil {
		snapshot = &Snapshot{}
	}
	if analysis == nil {
		analysis = &Analysis{}
	}
	if p.ApprovedManual == nil {
		p.ApprovedManual = map[ObjectID]bool{}
	}

	normalizedSnapshot := normalizeSnapshot(snapshot)
	fingerprint, err := p.fingerprint(normalizedSnapshot)
	if err != nil {
		return nil, err
	}
	planID := "plan-" + fingerprint[:12]
	createdAt := normalizedSnapshot.GeneratedAt
	if createdAt.IsZero() {
		// Keep the zero value explicit rather than introducing wall-clock drift.
		createdAt = time.Time{}
	}

	items := make([]PlanItem, 0, len(analysis.Order))
	for _, id := range analysis.Order {
		classification := analysis.Resources[id]
		item := PlanItem{
			ID:       id,
			Action:   ActionSkip,
			Strategy: StrategyNone,
			Reasons:  append([]string(nil), classification.Reasons...),
		}
		protection := p.Policy.Protect(lookupObject(normalizedSnapshot, id))
		item.Protection = append([]string(nil), protection.Reasons...)

		if classification.Garbage && !protection.Protected {
			item.Strategy = StrategyManual
			if classification.DirectlyReclaimable && !classification.RequiresApproval ||
				classification.RequiresApproval && p.ApprovedManual[id] {
				item.Action = ActionDelete
				if classification.RequiresApproval {
					item.Strategy = StrategyManual
					item.Reasons = append(item.Reasons, "manual-approval")
				} else {
					item.Strategy = StrategyAuto
				}
				item.IdempotencyKey = deriveIdempotencyKey(planID, id)
			} else {
				item.Action = ActionSkip
				item.Strategy = StrategyManual
				item.Reasons = append(item.Reasons, "manual-confirmation-required")
			}
		}
		sort.Strings(item.Reasons)
		sort.Strings(item.Protection)
		items = append(items, item)
	}
	sort.Slice(items, func(i, j int) bool {
		return items[i].ID.String() < items[j].ID.String()
	})

	return &Plan{
		PlanID:             planID,
		InputFingerprint:   fingerprint,
		CreatedAt:          createdAt,
		Items:              items,
		DanglingReferences: append([]DanglingReference(nil), analysis.DanglingReferences...),
	}, nil
}

// InputFingerprint computes the deterministic snapshot/policy/approval hash.
func (p *Planner) InputFingerprint(snapshot *Snapshot) (string, error) {
	if snapshot == nil {
		snapshot = &Snapshot{}
	}
	if p.ApprovedManual == nil {
		p.ApprovedManual = map[ObjectID]bool{}
	}
	return p.fingerprint(normalizeSnapshot(snapshot))
}

func (p *Planner) fingerprint(normalizedSnapshot Snapshot) (string, error) {
	return canonicalFingerprint(struct {
		Snapshot Snapshot   `json:"snapshot"`
		Policy   Policy     `json:"policy"`
		Approved []ObjectID `json:"approved"`
	}{
		Snapshot: normalizedSnapshot,
		Policy:   p.deterministicPolicy(),
		Approved: p.sortedApproved(),
	})
}

func (p *Planner) deterministicPolicy() Policy {
	policy := p.Policy
	policy.Now = time.Time{}
	policy.ProtectedIDs = append([]ObjectID(nil), policy.ProtectedIDs...)
	sort.Slice(policy.ProtectedIDs, func(i, j int) bool {
		return policy.ProtectedIDs[i].String() < policy.ProtectedIDs[j].String()
	})
	policy.Rules = append([]ProtectionRule(nil), policy.Rules...)
	sort.Slice(policy.Rules, func(i, j int) bool { return policy.Rules[i].ID < policy.Rules[j].ID })
	for i := range policy.Rules {
		policy.Rules[i].Selector.Kinds = append([]string(nil), policy.Rules[i].Selector.Kinds...)
		policy.Rules[i].Selector.Namespaces = append([]string(nil), policy.Rules[i].Selector.Namespaces...)
		sort.Strings(policy.Rules[i].Selector.Kinds)
		sort.Strings(policy.Rules[i].Selector.Namespaces)
	}
	return policy
}

func (p *Planner) sortedApproved() []ObjectID {
	ids := make([]ObjectID, 0, len(p.ApprovedManual))
	for id, approved := range p.ApprovedManual {
		if approved {
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i].String() < ids[j].String() })
	return ids
}

func normalizeSnapshot(in *Snapshot) Snapshot {
	out := *in
	out.Objects = append([]Object(nil), in.Objects...)
	out.References = append([]Edge(nil), in.References...)
	sort.Slice(out.Objects, func(i, j int) bool { return out.Objects[i].ID.String() < out.Objects[j].ID.String() })
	sort.Slice(out.References, func(i, j int) bool {
		if out.References[i].From.String() != out.References[j].From.String() {
			return out.References[i].From.String() < out.References[j].From.String()
		}
		if out.References[i].To.String() != out.References[j].To.String() {
			return out.References[i].To.String() < out.References[j].To.String()
		}
		return out.References[i].Relation < out.References[j].Relation
	})
	for i := range out.Objects {
		// Normalize nil and empty maps so semantically equal snapshots hash alike.
		if len(out.Objects[i].Labels) == 0 {
			out.Objects[i].Labels = nil
		}
		if len(out.Objects[i].Annotations) == 0 {
			out.Objects[i].Annotations = nil
		}
	}
	return out
}

func lookupObject(snapshot Snapshot, id ObjectID) Object {
	for _, obj := range snapshot.Objects {
		if obj.ID == id {
			return obj
		}
	}
	return Object{ID: id}
}

func canonicalFingerprint(value any) (string, error) {
	b, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}

func deriveIdempotencyKey(planID string, id ObjectID) string {
	sum := sha256.Sum256([]byte(planID + "|" + id.String()))
	return fmt.Sprintf("gc-%s-%s", planID[5:], hex.EncodeToString(sum[:8]))
}
