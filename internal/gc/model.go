package gc

import (
	"encoding/json"
	"time"
)

// ObjectID uniquely identifies a cluster resource.
type ObjectID struct {
	Kind      string `json:"kind"`
	Namespace string `json:"namespace,omitempty"`
	Name      string `json:"name"`
}

// String returns a stable JSON representation usable as a map key.
// JSON encoding prevents delimiter collisions when names contain "/" or ":".
func (id ObjectID) String() string {
	b, err := json.Marshal(id)
	if err != nil {
		// ObjectID contains only strings and cannot make json.Marshal fail.
		return ""
	}
	return string(b)
}

// IsZero reports whether the identifier is empty.
func (id ObjectID) IsZero() bool {
	return id.Kind == "" && id.Namespace == "" && id.Name == ""
}

// Object is the resource representation required by the garbage collector.
type Object struct {
	ID          ObjectID          `json:"id"`
	Owner       *ObjectID         `json:"owner,omitempty"`
	Root        bool              `json:"root,omitempty"`
	Labels      map[string]string `json:"labels,omitempty"`
	Annotations map[string]string `json:"annotations,omitempty"`
	CreatedAt   time.Time         `json:"createdAt,omitempty"`
	LastUsedAt  time.Time         `json:"lastUsedAt,omitempty"`
	// Class is reserved for future source-specific reclaim categories.
	Class string `json:"class,omitempty"`
}

// Edge describes a directed reference from one object to another.
type Edge struct {
	From     ObjectID `json:"from"`
	To       ObjectID `json:"to"`
	Relation string   `json:"relation,omitempty"`
}

// Snapshot is a point-in-time, implementation-independent resource view.
type Snapshot struct {
	GeneratedAt time.Time `json:"generatedAt,omitempty"`
	Objects     []Object  `json:"objects"`
	References  []Edge    `json:"references,omitempty"`
}

// DanglingReference describes a reference whose target does not exist.
type DanglingReference struct {
	From     ObjectID `json:"from"`
	To       ObjectID `json:"to"`
	Relation string   `json:"relation,omitempty"`
}

// Classification explains why a resource is reachable or reclaimable.
type Classification struct {
	ID                  ObjectID
	Reachable           bool
	Garbage             bool
	DirectlyReclaimable bool
	RequiresApproval    bool
	Reasons             []string
	ReferencedBy        []ObjectID
	References          []ObjectID
}

// Analysis is the deterministic result of a graph walk.
type Analysis struct {
	Order              []ObjectID
	Resources          map[ObjectID]Classification
	DanglingReferences []DanglingReference
}
