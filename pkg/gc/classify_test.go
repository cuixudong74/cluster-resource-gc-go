package gc

import (
	"testing"
	"time"
)

var testNow = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func res(id ObjectID, kind string, owner ObjectID, refs ...ObjectID) Resource {
	return Resource{
		ID: id, Kind: kind, Name: string(id), Owner: owner, Refs: refs,
		CreatedAt: testNow.Add(-48 * time.Hour),
		LastUsed:  testNow.Add(-48 * time.Hour),
	}
}

func findClass(t *testing.T, cs []Classification, id ObjectID) Classification {
	t.Helper()
	for _, c := range cs {
		if c.ID == id {
			return c
		}
	}
	t.Fatalf("classification for %s not found", id)
	return Classification{}
}

func TestClassifyCycleDoesNotHang(t *testing.T) {
	// a -> b -> c -> a 构成环，且整体不可达，应全部判为垃圾且不死循环。
	snap := &Snapshot{Resources: []Resource{
		res("root", "Namespace", ""),
		res("a", "Pod", "", "b"),
		res("b", "Pod", "", "c"),
		res("c", "Pod", "", "a"),
	}}
	classes := Classify(snap, RootSet{RootIDs: []ObjectID{"root"}},
		ClassifyOptions{Now: testNow})
	for _, id := range []ObjectID{"a", "b", "c"} {
		c := findClass(t, classes, id)
		if !c.Garbage {
			t.Fatalf("%s should be garbage, got %+v", id, c)
		}
		if c.Reason != ReasonGarbageOwned {
			t.Fatalf("%s reason = %s, want %s", id, c.Reason, ReasonGarbageOwned)
		}
	}
	if findClass(t, classes, "root").Garbage {
		t.Fatal("root must not be garbage")
	}
}

func TestClassifyReachableViaOwnership(t *testing.T) {
	// deploy 是根，pod 归属于 deploy，pod 引用 cm：都可达。
	snap := &Snapshot{Resources: []Resource{
		res("deploy", "Deployment", ""),
		res("pod", "Pod", "deploy"),
		res("cm", "ConfigMap", ""),
	}}
	snap.Resources[1].Refs = []ObjectID{"cm"}
	classes := Classify(snap, RootSet{RootIDs: []ObjectID{"deploy"}},
		ClassifyOptions{Now: testNow})
	for _, id := range []ObjectID{"deploy", "pod", "cm"} {
		if findClass(t, classes, id).Garbage {
			t.Fatalf("%s must stay reachable", id)
		}
	}
}

func TestClassifyDanglingRef(t *testing.T) {
	snap := &Snapshot{Resources: []Resource{
		res("root", "Namespace", ""),
		res("orphan", "Pod", "", "missing-cm"),
	}}
	classes := Classify(snap, RootSet{RootIDs: []ObjectID{"root"}},
		ClassifyOptions{Now: testNow})
	c := findClass(t, classes, "orphan")
	if !c.Garbage || c.Reason != ReasonDanglingRef {
		t.Fatalf("want dangling garbage, got %+v", c)
	}
	if c.Category != CategoryManual {
		t.Fatalf("dangling should require manual confirmation, got %s", c.Category)
	}
}

func TestClassifyGarbageOnlyReferenced(t *testing.T) {
	// mid 只被垃圾对象 junk 引用，自身也不可达。
	snap := &Snapshot{Resources: []Resource{
		res("root", "Namespace", ""),
		res("junk", "Pod", "", "mid"),
		res("mid", "ConfigMap", ""),
	}}
	classes := Classify(snap, RootSet{RootIDs: []ObjectID{"root"}},
		ClassifyOptions{Now: testNow})
	c := findClass(t, classes, "mid")
	if !c.Garbage || c.Reason != ReasonGarbageOwned {
		t.Fatalf("want garbage-only-referenced, got %+v", c)
	}
}

func TestClassifyIdle(t *testing.T) {
	idle := res("idle-pv", "PersistentVolume", "")
	idle.LastUsed = testNow.Add(-90 * 24 * time.Hour)
	snap := &Snapshot{Resources: []Resource{
		res("root", "Namespace", ""),
		idle,
	}}
	classes := Classify(snap, RootSet{RootIDs: []ObjectID{"root"}},
		ClassifyOptions{Now: testNow, IdleThreshold: 30 * 24 * time.Hour})
	c := findClass(t, classes, "idle-pv")
	if !c.Garbage || c.Reason != ReasonIdle {
		t.Fatalf("want idle garbage, got %+v", c)
	}
	if c.Category != CategoryAuto {
		t.Fatalf("idle should be auto-recyclable, got %s", c.Category)
	}
}

func TestClassifyDeterministic(t *testing.T) {
	snap := &Snapshot{Resources: []Resource{
		res("root", "Namespace", ""),
		res("z", "Pod", ""), res("m", "Pod", ""), res("a", "Pod", ""),
	}}
	roots := RootSet{RootIDs: []ObjectID{"root"}}
	opts := ClassifyOptions{Now: testNow}
	c1 := Classify(snap, roots, opts)
	c2 := Classify(snap, roots, opts)
	if len(c1) != len(c2) {
		t.Fatal("length mismatch")
	}
	for i := range c1 {
		if c1[i] != c2[i] {
			t.Fatalf("non-deterministic at %d: %+v vs %+v", i, c1[i], c2[i])
		}
	}
}
