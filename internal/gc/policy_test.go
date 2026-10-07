package gc

import (
	"context"
	"testing"
	"time"
)

func TestProtectionRulesBlockPlannedDelete(t *testing.T) {
	now := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
	protectedByLabel := ObjectID{Kind: "ConfigMap", Namespace: "ns", Name: "label"}
	protectedByGrace := ObjectID{Kind: "ConfigMap", Namespace: "ns", Name: "new"}
	protectedByRule := ObjectID{Kind: "Secret", Namespace: "prod", Name: "credential"}
	explicit := ObjectID{Kind: "ConfigMap", Namespace: "ns", Name: "explicit"}
	deletable := ObjectID{Kind: "ConfigMap", Namespace: "ns", Name: "old"}
	snapshot := &Snapshot{
		GeneratedAt: now,
		Objects: []Object{
			{ID: protectedByLabel, Labels: map[string]string{"keep": "true"}},
			{ID: protectedByGrace, CreatedAt: now.Add(-time.Minute)},
			{ID: protectedByRule},
			{ID: explicit},
			{ID: deletable, CreatedAt: now.Add(-30 * 24 * time.Hour)},
		},
	}
	policy := Policy{
		Now:                 now,
		GracePeriod:         PolicyDuration(time.Hour),
		ProtectedIDs:        []ObjectID{explicit},
		ProtectedLabel:      "keep",
		ProtectedAnnotation: "keep",
		Rules: []ProtectionRule{{
			ID:       "protect-prod-secrets",
			Selector: Selector{Kinds: []string{"Secret"}, Namespaces: []string{"prod"}},
		}},
	}
	analysis, err := (&Analyzer{Now: now}).Analyze(context.Background(), snapshot)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := (&Planner{Policy: policy}).DryRun(snapshot, analysis)
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		id     ObjectID
		reason string
	}{
		{protectedByLabel, ReasonProtectedLabel},
		{protectedByGrace, ReasonGracePeriod},
		{protectedByRule, "protection-rule:protect-prod-secrets"},
		{explicit, ReasonExplicitlyProtected},
	}
	for _, tc := range cases {
		item := planItem(plan, tc.id)
		if item.Action != ActionSkip {
			t.Fatalf("%s should be protected, got action %s", tc.id, item.Action)
		}
		if !containsString(item.Protection, tc.reason) {
			t.Fatalf("%s missing reason %s in %#v", tc.id, tc.reason, item.Protection)
		}
	}
	if item := planItem(plan, deletable); item.Action != ActionDelete {
		t.Fatalf("unprotected garbage should delete, got %+v", item)
	}
}
