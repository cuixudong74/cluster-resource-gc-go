package gc

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"sync"
)

var (
	// ErrDeletionNotSupported indicates a read-only source.
	ErrDeletionNotSupported = errors.New("deletion is not supported by this source")
	// ErrObjectNotFound is returned when a source cannot locate a resource.
	ErrObjectNotFound = errors.New("object not found")
)

// DeleteRequest is an idempotent resource deletion request.
type DeleteRequest struct {
	ID             ObjectID
	Attempt        int
	IdempotencyKey string
}

// DeleteResult describes the source's atomic deletion outcome.
type DeleteResult struct {
	Deleted        bool
	AlreadyDeleted bool
}

// Source isolates the collector from Kubernetes, a file snapshot, or a fake.
type Source interface {
	Load(context.Context) (*Snapshot, error)
	Delete(context.Context, DeleteRequest) (DeleteResult, error)
}

// MemorySource is a deterministic simulated source used by local runs/tests.
type MemorySource struct {
	mu           sync.Mutex
	snapshot     Snapshot
	deleted      map[string]bool
	deletedByKey map[string]bool
	attempts     []DeleteRequest
}

// NewMemorySource clones a snapshot so callers cannot mutate source state.
func NewMemorySource(snapshot *Snapshot) *MemorySource {
	s := &MemorySource{deleted: make(map[string]bool), deletedByKey: make(map[string]bool)}
	if snapshot != nil {
		s.snapshot = cloneSnapshot(snapshot)
	}
	return s
}

// Load returns a defensive copy of the simulated snapshot.
func (s *MemorySource) Load(context.Context) (*Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	got := cloneSnapshot(&s.snapshot)
	kept := got.Objects[:0]
	for _, obj := range got.Objects {
		if !s.deleted[obj.ID.String()] {
			kept = append(kept, obj)
		}
	}
	got.Objects = kept
	filteredReferences := got.References[:0]
	for _, edge := range got.References {
		if !s.deleted[edge.From.String()] && !s.deleted[edge.To.String()] {
			filteredReferences = append(filteredReferences, edge)
		}
	}
	got.References = filteredReferences
	return &got, nil
}

// Delete atomically applies an idempotent delete in memory.
func (s *MemorySource) Delete(_ context.Context, req DeleteRequest) (DeleteResult, error) {
	if req.ID.IsZero() {
		return DeleteResult{}, ErrObjectNotFound
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	s.attempts = append(s.attempts, req)
	key := req.ID.String()
	if req.IdempotencyKey != "" && s.deletedByKey[req.IdempotencyKey] {
		return DeleteResult{AlreadyDeleted: true}, nil
	}
	if s.deleted[key] {
		return DeleteResult{AlreadyDeleted: true}, nil
	}
	for _, obj := range s.snapshot.Objects {
		if obj.ID == req.ID {
			s.deleted[key] = true
			if req.IdempotencyKey != "" {
				s.deletedByKey[req.IdempotencyKey] = true
			}
			return DeleteResult{Deleted: true}, nil
		}
	}
	// The local source models idempotent deletion: an absent object during
	// replay is already deleted. A real adapter should distinguish this from a
	// malformed/unplanned identifier before reporting idempotent success.
	return DeleteResult{AlreadyDeleted: true}, nil
}

// AttemptCount returns the number of source-level delete calls.
func (s *MemorySource) AttemptCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.attempts)
}

// FileSource reads a JSON snapshot from the local filesystem.
type FileSource struct {
	Path string
}

// Load decodes a local JSON snapshot.
func (s *FileSource) Load(context.Context) (*Snapshot, error) {
	b, err := os.ReadFile(s.Path)
	if err != nil {
		return nil, err
	}
	var snapshot Snapshot
	if err := json.Unmarshal(b, &snapshot); err != nil {
		return nil, err
	}
	return &snapshot, nil
}

// Delete is intentionally unsupported for a read-only snapshot source.
func (s *FileSource) Delete(context.Context, DeleteRequest) (DeleteResult, error) {
	return DeleteResult{}, ErrDeletionNotSupported
}

func cloneSnapshot(in *Snapshot) Snapshot {
	if in == nil {
		return Snapshot{}
	}
	out := *in
	if in.Objects != nil {
		out.Objects = append([]Object(nil), in.Objects...)
		for i := range out.Objects {
			if in.Objects[i].Labels != nil {
				out.Objects[i].Labels = cloneStringMap(in.Objects[i].Labels)
			}
			if in.Objects[i].Annotations != nil {
				out.Objects[i].Annotations = cloneStringMap(in.Objects[i].Annotations)
			}
			if in.Objects[i].Owner != nil {
				owner := *in.Objects[i].Owner
				out.Objects[i].Owner = &owner
			}
		}
	}
	if in.References != nil {
		out.References = append([]Edge(nil), in.References...)
	}
	return out
}

func cloneStringMap(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
