package gc

import (
	"context"
	"testing"
)

func TestOwnerRelationshipsAndDanglingOwner(t *testing.T) {
	owner := ObjectID{Kind: "Deployment", Namespace: "ns", Name: "live"}
	child := ObjectID{Kind: "ReplicaSet", Namespace: "ns", Name: "child"}
	missing := ObjectID{Kind: "Deployment", Namespace: "ns", Name: "missing"}
	orphan := ObjectID{Kind: "ReplicaSet", Namespace: "ns", Name: "orphan"}
	snapshot := &Snapshot{
		Objects: []Object{
			{ID: owner, Root: true},
			{ID: child, Owner: &owner},
			{ID: orphan, Owner: &missing},
		},
	}
	analysis, err := (&Analyzer{}).Analyze(context.Background(), snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if !analysis.Resources[child].Reachable || analysis.Resources[child].Garbage {
		t.Fatalf("owned child must remain reachable: %+v", analysis.Resources[child])
	}
	c := analysis.Resources[orphan]
	if c.Reachable || !c.Garbage || !c.RequiresApproval {
		t.Fatalf("orphan with missing owner should be manual garbage: %+v", c)
	}
	if !containsString(c.Reasons, ReasonDanglingReference) {
		t.Fatalf("missing dangling owner reason: %#v", c.Reasons)
	}
	if len(analysis.DanglingReferences) != 1 || analysis.DanglingReferences[0].Relation != "owner-ref" {
		t.Fatalf("unexpected dangling owner references: %+v", analysis.DanglingReferences)
	}
}
