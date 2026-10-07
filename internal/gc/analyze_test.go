package gc

import (
	"context"
	"reflect"
	"testing"
	"time"
)

func TestAnalyzeCyclesDoNotLoopAndClassifyGarbage(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	snapshot := &Snapshot{
		Objects: []Object{
			{ID: ObjectID{Kind: "Namespace", Name: "live"}, Root: true},
			{ID: ObjectID{Kind: "ConfigMap", Namespace: "live", Name: "a"}},
			{ID: ObjectID{Kind: "ConfigMap", Namespace: "live", Name: "b"}},
			{ID: ObjectID{Kind: "ConfigMap", Namespace: "dead", Name: "c"}},
			{ID: ObjectID{Kind: "ConfigMap", Namespace: "dead", Name: "d"}},
		},
		References: []Edge{
			{From: ObjectID{Kind: "Namespace", Name: "live"}, To: ObjectID{Kind: "ConfigMap", Namespace: "live", Name: "a"}},
			{From: ObjectID{Kind: "ConfigMap", Namespace: "live", Name: "a"}, To: ObjectID{Kind: "ConfigMap", Namespace: "live", Name: "b"}},
			{From: ObjectID{Kind: "ConfigMap", Namespace: "live", Name: "b"}, To: ObjectID{Kind: "ConfigMap", Namespace: "live", Name: "a"}},
			{From: ObjectID{Kind: "ConfigMap", Namespace: "dead", Name: "c"}, To: ObjectID{Kind: "ConfigMap", Namespace: "dead", Name: "d"}},
			{From: ObjectID{Kind: "ConfigMap", Namespace: "dead", Name: "d"}, To: ObjectID{Kind: "ConfigMap", Namespace: "dead", Name: "c"}},
		},
	}

	analysis, err := (&Analyzer{Now: now}).Analyze(context.Background(), snapshot)
	if err != nil {
		t.Fatalf("Analyze returned error: %v", err)
	}
	for _, name := range []string{"a", "b"} {
		id := ObjectID{Kind: "ConfigMap", Namespace: "live", Name: name}
		if !analysis.Resources[id].Reachable || analysis.Resources[id].Garbage {
			t.Fatalf("%s should be reachable through live cycle: %+v", name, analysis.Resources[id])
		}
	}
	for _, name := range []string{"c", "d"} {
		id := ObjectID{Kind: "ConfigMap", Namespace: "dead", Name: name}
		c := analysis.Resources[id]
		if c.Reachable || !c.Garbage || !c.DirectlyReclaimable {
			t.Fatalf("%s should be directly reclaimable garbage: %+v", name, c)
		}
		if !containsString(c.Reasons, ReasonOnlyGarbageReferencer) {
			t.Fatalf("%s should identify only garbage referencers, got %#v", name, c.Reasons)
		}
	}
}

func TestAnalyzeDanglingAndIdleRequireManual(t *testing.T) {
	now := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
	dangling := ObjectID{Kind: "Secret", Namespace: "dead", Name: "missing"}
	idle := ObjectID{Kind: "PVC", Namespace: "dead", Name: "idle"}
	snapshot := &Snapshot{
		Objects: []Object{
			{ID: ObjectID{Kind: "Pod", Namespace: "dead", Name: "broken"}, LastUsedAt: now},
			{ID: idle, LastUsedAt: now.Add(-2 * time.Hour)},
		},
		References: []Edge{
			{From: ObjectID{Kind: "Pod", Namespace: "dead", Name: "broken"}, To: dangling},
		},
	}

	analysis, err := (&Analyzer{Now: now, IdleThreshold: PolicyDuration(time.Hour)}).Analyze(context.Background(), snapshot)
	if err != nil {
		t.Fatalf("Analyze returned error: %v", err)
	}
	broken := analysis.Resources[ObjectID{Kind: "Pod", Namespace: "dead", Name: "broken"}]
	if !broken.RequiresApproval || broken.DirectlyReclaimable {
		t.Fatalf("dangling holder should require manual approval: %+v", broken)
	}
	if !containsString(broken.Reasons, ReasonDanglingReference) {
		t.Fatalf("missing dangling reason: %#v", broken.Reasons)
	}
	idleClass := analysis.Resources[idle]
	if !idleClass.RequiresApproval || idleClass.DirectlyReclaimable {
		t.Fatalf("long-idle object should require manual approval: %+v", idleClass)
	}
	if !containsString(idleClass.Reasons, ReasonLongIdle) {
		t.Fatalf("missing idle reason: %#v", idleClass.Reasons)
	}
	if len(analysis.DanglingReferences) != 1 || analysis.DanglingReferences[0].To != dangling {
		t.Fatalf("unexpected dangling references: %+v", analysis.DanglingReferences)
	}
}

func TestPlanIsDeterministicAndProtectsReachable(t *testing.T) {
	now := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
	liveID := ObjectID{Kind: "Namespace", Name: "live"}
	garbageID := ObjectID{Kind: "ConfigMap", Namespace: "live", Name: "old"}
	snapshot := &Snapshot{
		GeneratedAt: now,
		Objects: []Object{
			{ID: garbageID},
			{ID: liveID, Root: true},
		},
	}
	policy := Policy{
		Now:                 now,
		ProtectedLabel:      "resource-gc/protected",
		ProtectedAnnotation: "resource-gc/protected",
	}
	planner := &Planner{Policy: policy}
	analysis, err := (&Analyzer{Now: now}).Analyze(context.Background(), snapshot)
	if err != nil {
		t.Fatal(err)
	}
	first, err := planner.DryRun(snapshot, analysis)
	if err != nil {
		t.Fatal(err)
	}
	second, err := planner.DryRun(snapshot, analysis)
	if err != nil {
		t.Fatal(err)
	}
	if first.PlanID != second.PlanID || first.InputFingerprint != second.InputFingerprint {
		t.Fatalf("plans changed across identical runs: %s != %s", first.PlanID, second.PlanID)
	}
	if !reflect.DeepEqual(first.Items, second.Items) {
		t.Fatalf("plan items differ:\n%#v\n%#v", first.Items, second.Items)
	}
	if item := planItem(first, garbageID); item.Action != ActionDelete || item.Strategy != StrategyAuto {
		t.Fatalf("garbage should auto delete: %+v", item)
	}
	if item := planItem(first, liveID); item.Action != ActionSkip {
		t.Fatalf("reachable root should skip: %+v", item)
	}
}

func planItem(plan *Plan, id ObjectID) PlanItem {
	for _, item := range plan.Items {
		if item.ID == id {
			return item
		}
	}
	return PlanItem{}
}

func TestNormalizationMakesEmptyMapsEquivalent(t *testing.T) {
	base := &Snapshot{Objects: []Object{{ID: ObjectID{Kind: "ConfigMap", Name: "x"}, Labels: map[string]string{}}}}
	same := &Snapshot{Objects: []Object{{ID: ObjectID{Kind: "ConfigMap", Name: "x"}, Labels: nil}}}
	analysisA, err := (&Analyzer{}).Analyze(context.Background(), base)
	if err != nil {
		t.Fatal(err)
	}
	analysisB, err := (&Analyzer{}).Analyze(context.Background(), same)
	if err != nil {
		t.Fatal(err)
	}
	planner := &Planner{Policy: Policy{}}
	a, err := planner.DryRun(base, analysisA)
	if err != nil {
		t.Fatal(err)
	}
	b, err := planner.DryRun(same, analysisB)
	if err != nil {
		t.Fatal(err)
	}
	if a.InputFingerprint != b.InputFingerprint {
		t.Fatalf("empty and nil metadata maps should normalize to same fingerprint: %s != %s", a.InputFingerprint, b.InputFingerprint)
	}
}
