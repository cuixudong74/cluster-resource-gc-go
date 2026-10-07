package gc

import (
	"context"
	"sort"
	"time"
)

const (
	ReasonReachable             = "reachable-from-root"
	ReasonUnreachable           = "unreachable-from-root"
	ReasonDanglingReference     = "dangling-reference"
	ReasonOnlyGarbageReferencer = "only-referenced-by-garbage"
	ReasonLongIdle              = "long-idle"
)

// Analyzer performs reachability and garbage classification.
type Analyzer struct {
	// Roots supplements objects explicitly marked with Root=true.
	Roots []ObjectID
	// Now is injected to keep analysis deterministic in tests.
	Now time.Time
	// IdleThreshold marks an object long-idle when LastUsedAt exceeds it.
	// Zero LastUsedAt is treated as unknown, never as proof of idleness.
	IdleThreshold PolicyDuration
}

// Analyze walks owner/reference edges without recursing through cycles.
func (a *Analyzer) Analyze(ctx context.Context, snapshot *Snapshot) (*Analysis, error) {
	if snapshot == nil {
		snapshot = &Snapshot{}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	objects := make(map[ObjectID]Object, len(snapshot.Objects))
	order := make([]ObjectID, 0, len(snapshot.Objects))
	for _, obj := range snapshot.Objects {
		if obj.ID.IsZero() {
			continue
		}
		if _, seen := objects[obj.ID]; !seen {
			objects[obj.ID] = obj
			order = append(order, obj.ID)
		}
	}
	sort.Slice(order, func(i, j int) bool { return order[i].String() < order[j].String() })

	out := make(map[ObjectID][]ObjectID)
	in := make(map[ObjectID][]ObjectID)
	dangling := make([]DanglingReference, 0)
	danglingFrom := make(map[ObjectID]bool)

	addEdge := func(from, to ObjectID, relation string) {
		if from.IsZero() || to.IsZero() {
			return
		}
		if _, exists := objects[to]; !exists {
			dangling = append(dangling, DanglingReference{From: from, To: to, Relation: relation})
			if _, exists := objects[from]; exists {
				danglingFrom[from] = true
			}
			return
		}
		if _, exists := objects[from]; !exists {
			return
		}
		out[from] = append(out[from], to)
		in[to] = append(in[to], from)
	}

	for _, id := range order {
		obj := objects[id]
		if obj.Owner != nil {
			// Owner references are encoded child -> owner. Treat ownership as a
			// bidirectional safety edge: a live owner reaches its dependents,
			// and a live dependent does not orphan its declared owner.
			if _, ownerExists := objects[*obj.Owner]; ownerExists {
				addEdge(id, *obj.Owner, "owner-ref")
				addEdge(*obj.Owner, id, "owner-dependent")
			} else {
				dangling = append(dangling, DanglingReference{From: id, To: *obj.Owner, Relation: "owner-ref"})
				danglingFrom[id] = true
			}
		}
	}
	for _, edge := range snapshot.References {
		addEdge(edge.From, edge.To, edge.Relation)
	}
	for id := range out {
		out[id] = uniqueSortedIDs(out[id])
	}
	for id := range in {
		in[id] = uniqueSortedIDs(in[id])
	}
	sort.Slice(dangling, func(i, j int) bool {
		if dangling[i].From.String() != dangling[j].From.String() {
			return dangling[i].From.String() < dangling[j].From.String()
		}
		if dangling[i].To.String() != dangling[j].To.String() {
			return dangling[i].To.String() < dangling[j].To.String()
		}
		return dangling[i].Relation < dangling[j].Relation
	})
	dangling = uniqueDangling(dangling)

	reachable := make(map[ObjectID]bool)
	stack := make([]ObjectID, 0, len(order))
	pushRoot := func(id ObjectID) {
		if _, exists := objects[id]; exists && !reachable[id] {
			reachable[id] = true
			stack = append(stack, id)
		}
	}
	for _, id := range order {
		if objects[id].Root {
			pushRoot(id)
		}
	}
	for _, id := range a.Roots {
		pushRoot(id)
	}

	// Iterative DFS: the visited set makes cycles terminate and shared nodes
	// are processed once rather than repeatedly counted.
	for len(stack) > 0 {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		current := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		for _, next := range out[current] {
			if !reachable[next] {
				reachable[next] = true
				stack = append(stack, next)
			}
		}
	}

	resources := make(map[ObjectID]Classification, len(order))
	for _, id := range order {
		obj := objects[id]
		c := Classification{
			ID:           id,
			Reachable:    reachable[id],
			ReferencedBy: append([]ObjectID(nil), in[id]...),
			References:   append([]ObjectID(nil), out[id]...),
		}
		if c.Reachable {
			c.Reasons = append(c.Reasons, ReasonReachable)
		} else {
			c.Garbage = true
			c.DirectlyReclaimable = true
			c.Reasons = append(c.Reasons, ReasonUnreachable)

			hasIncoming := len(in[id]) > 0
			allIncomingGarbage := hasIncoming
			for _, from := range in[id] {
				if reachable[from] {
					allIncomingGarbage = false
					break
				}
			}
			if allIncomingGarbage {
				c.Reasons = append(c.Reasons, ReasonOnlyGarbageReferencer)
			}
		}

		if danglingFrom[id] {
			c.Reasons = append(c.Reasons, ReasonDanglingReference)
			if c.Garbage {
				// A missing target can mean stale status data, a broken owner,
				// or a snapshot race; deleting the holder requires a human.
				c.DirectlyReclaimable = false
				c.RequiresApproval = true
			}
		}
		if a.isLongIdle(obj) {
			c.Reasons = append(c.Reasons, ReasonLongIdle)
			if c.Garbage {
				// Inactivity is a weaker signal than graph reachability.
				c.DirectlyReclaimable = false
				c.RequiresApproval = true
			}
		}
		c.Reasons = uniqueStrings(c.Reasons)
		resources[id] = c
	}

	return &Analysis{
		Order:              order,
		Resources:          resources,
		DanglingReferences: dangling,
	}, nil
}

func (a *Analyzer) isLongIdle(obj Object) bool {
	idleThreshold := a.IdleThreshold.Duration()
	if idleThreshold <= 0 || a.Now.IsZero() || obj.LastUsedAt.IsZero() {
		return false
	}
	return a.Now.Sub(obj.LastUsedAt) >= idleThreshold
}

func uniqueSortedIDs(ids []ObjectID) []ObjectID {
	if len(ids) == 0 {
		return nil
	}
	seen := make(map[string]bool, len(ids))
	out := make([]ObjectID, 0, len(ids))
	for _, id := range ids {
		key := id.String()
		if !seen[key] {
			seen[key] = true
			out = append(out, id)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].String() < out[j].String() })
	return out
}

func uniqueDangling(in []DanglingReference) []DanglingReference {
	seen := make(map[string]bool, len(in))
	out := make([]DanglingReference, 0, len(in))
	for _, item := range in {
		key := item.From.String() + "\x00" + item.To.String() + "\x00" + item.Relation
		if !seen[key] {
			seen[key] = true
			out = append(out, item)
		}
	}
	return out
}

func uniqueStrings(in []string) []string {
	seen := make(map[string]bool, len(in))
	out := make([]string, 0, len(in))
	for _, item := range in {
		if !seen[item] {
			seen[item] = true
			out = append(out, item)
		}
	}
	return out
}
