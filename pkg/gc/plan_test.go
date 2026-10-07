package gc

import (
	"testing"
	"time"
)

func TestProtectPolicy(t *testing.T) {
	p := &ProtectPolicy{
		ProtectedLabel: "gc.io/protect",
		GracePeriod:    24 * time.Hour,
		Rules: []ProtectRule{
			{Name: "protect-kube-system", Namespace: "kube-system"},
			{Name: "protect-critical", Labels: map[string]string{"critical": "true"}},
		},
		Now: testNow,
	}
	cases := []struct {
		name string
		r    Resource
		want bool
	}{
		{"labeled", Resource{ID: "a", Labels: map[string]string{"gc.io/protect": "true"}}, true},
		{"grace", Resource{ID: "b", CreatedAt: testNow.Add(-1 * time.Hour)}, true},
		{"rule-ns", Resource{ID: "c", Namespace: "kube-system", CreatedAt: testNow.Add(-72 * time.Hour)}, true},
		{"rule-label", Resource{ID: "d", Labels: map[string]string{"critical": "true"}, CreatedAt: testNow.Add(-72 * time.Hour)}, true},
		{"unprotected", Resource{ID: "e", CreatedAt: testNow.Add(-72 * time.Hour)}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := p.Check(&tc.r)
			if got.Protected != tc.want {
				t.Fatalf("protected=%v want %v (reason %q)", got.Protected, tc.want, got.Reason)
			}
			if got.Protected && got.Reason == "" {
				t.Fatal("protected object must carry an explainable reason")
			}
		})
	}
}

func TestBuildPlanDeterministic(t *testing.T) {
	snap := &Snapshot{Resources: []Resource{
		res("root", "Namespace", ""),
		res("g3", "Pod", ""), res("g1", "Pod", ""), res("g2", "Pod", ""),
	}}
	opts := PlanOptions{
		BatchSize: 2,
		Policy:    &ProtectPolicy{Now: testNow},
		Classify:  ClassifyOptions{Now: testNow},
	}
	roots := RootSet{RootIDs: []ObjectID{"root"}}
	p1, err := BuildPlan(snap, roots, opts)
	if err != nil {
		t.Fatal(err)
	}
	p2, err := BuildPlan(snap, roots, opts)
	if err != nil {
		t.Fatal(err)
	}
	if p1.ID != p2.ID {
		t.Fatalf("plan ID not deterministic: %s vs %s", p1.ID, p2.ID)
	}
	if len(p1.Items) != 3 {
		t.Fatalf("want 3 items, got %d", len(p1.Items))
	}
	// 项必须按 ID 排序。
	for i := 0; i+1 < len(p1.Items); i++ {
		if p1.Items[i].ID >= p1.Items[i+1].ID {
			t.Fatal("items not sorted by ID")
		}
	}
	if len(p1.Batches) != 2 || len(p1.Batches[0]) != 2 || len(p1.Batches[1]) != 1 {
		t.Fatalf("unexpected batches: %v", p1.Batches)
	}
}

func TestBuildPlanProtectsAndSkipsManual(t *testing.T) {
	protected := res("protected", "Pod", "")
	protected.Labels = map[string]string{"keep": "yes"}
	dangling := res("dangling", "Pod", "", "ghost")
	snap := &Snapshot{Resources: []Resource{
		res("root", "Namespace", ""),
		res("plain", "Pod", ""),
		protected,
		dangling,
	}}
	plan, err := BuildPlan(snap, RootSet{RootIDs: []ObjectID{"root"}}, PlanOptions{
		Policy: &ProtectPolicy{
			Rules: []ProtectRule{{Name: "keep", Labels: map[string]string{"keep": "yes"}}},
			Now:   testNow,
		},
		Classify: ClassifyOptions{Now: testNow},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Items) != 1 || plan.Items[0].ID != "plain" {
		t.Fatalf("want only plain in plan, got %+v", plan.Items)
	}
	if len(plan.Skipped) != 2 {
		t.Fatalf("want 2 skipped, got %+v", plan.Skipped)
	}
	// 悬空引用默认需人工确认，不进计划。
	for _, s := range plan.Skipped {
		if s.ID == "dangling" && s.Reason == "" {
			t.Fatal("skipped dangling item must carry reason")
		}
	}
}
