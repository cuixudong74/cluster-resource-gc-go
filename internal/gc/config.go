package gc

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"time"
)

// Default local configuration values.
const (
	DefaultStateDir      = ".gc-state"
	DefaultAuditPath     = ".gc-state/audit.jsonl"
	DefaultRPS           = 5.0
	DefaultConfigTimeout = 10 * time.Minute
	DefaultDeleteRole    = "gc-admin"
	SourceFile           = "file"
	SourceMemoryFromFile = "memory-file"
)

// Config configures source, protection, scheduling, and recovery.
type Config struct {
	// Source is "file" for read-only snapshots or "memory-file" for a process
	// local, mutable simulated source initialized from SnapshotPath.
	Source            string     `json:"source,omitempty"`
	SnapshotPath      string     `json:"snapshotPath,omitempty"`
	StateDir          string     `json:"stateDir,omitempty"`
	AuditPath         string     `json:"auditPath,omitempty"`
	DryRun            bool       `json:"dryRun"`
	ResumePlanID      string     `json:"resumePlanId,omitempty"`
	Concurrency       int        `json:"concurrency,omitempty"`
	BatchSize         int        `json:"batchSize,omitempty"`
	MaxAttempts       int        `json:"maxAttempts,omitempty"`
	RequestsPerSecond float64    `json:"requestsPerSecond,omitempty"`
	Timeout           Duration   `json:"timeout,omitempty"`
	Principal         Principal  `json:"principal"`
	Policy            Policy     `json:"policy"`
	ApprovedManual    []ObjectID `json:"approvedManual,omitempty"`
}

// Duration wraps time.Duration so JSON accepts "30m" in addition to nanos.
type Duration time.Duration

// Duration returns the wrapped standard-library duration.
func (d Duration) Duration() time.Duration { return time.Duration(d) }

// UnmarshalJSON accepts a duration string or numeric nanosecond value.
func (d *Duration) UnmarshalJSON(b []byte) error {
	var text string
	if err := json.Unmarshal(b, &text); err == nil {
		parsed, err := time.ParseDuration(text)
		if err != nil {
			return err
		}
		*d = Duration(parsed)
		return nil
	}
	var nanos int64
	if err := json.Unmarshal(b, &nanos); err != nil {
		return err
	}
	*d = Duration(nanos)
	return nil
}

// MarshalJSON renders the duration as a human-readable string.
func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(d).String())
}

// LoadConfig reads JSON configuration from path and applies safe defaults.
func LoadConfig(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg Config
	if err := json.Unmarshal(b, &cfg); err != nil {
		return nil, err
	}
	cfg.defaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func (c *Config) defaults() {
	if c.Source == "" {
		c.Source = SourceMemoryFromFile
	}
	if c.StateDir == "" {
		c.StateDir = DefaultStateDir
	}
	if c.AuditPath == "" {
		c.AuditPath = DefaultAuditPath
	}
	if c.Concurrency <= 0 {
		c.Concurrency = DefaultConcurrency
	}
	if c.BatchSize <= 0 {
		c.BatchSize = DefaultBatchSize
	}
	if c.MaxAttempts <= 0 {
		c.MaxAttempts = DefaultMaxAttempts
	}
	if c.RequestsPerSecond <= 0 {
		c.RequestsPerSecond = DefaultRPS
	}
	if c.Timeout <= 0 {
		c.Timeout = Duration(DefaultConfigTimeout)
	}
	if c.Policy.ProtectedLabel == "" {
		c.Policy.ProtectedLabel = "resource-gc/protected"
	}
}

// Validate rejects unsafe or incomplete local execution configuration.
func (c *Config) Validate() error {
	if c.SnapshotPath == "" {
		return errors.New("snapshotPath is required")
	}
	switch c.Source {
	case SourceFile, SourceMemoryFromFile:
	default:
		return errors.New("source must be file or memory-file")
	}
	if c.Concurrency < 0 {
		return errors.New("concurrency must be positive")
	}
	if c.BatchSize < 0 {
		return errors.New("batchSize must be positive")
	}
	if c.MaxAttempts < 0 {
		return errors.New("maxAttempts must be positive")
	}
	if c.RequestsPerSecond < 0 {
		return errors.New("requestsPerSecond must be positive")
	}
	return nil
}

// ApprovedManualSet converts the approved list into a lookup set.
func (c *Config) ApprovedManualSet() map[ObjectID]bool {
	out := make(map[ObjectID]bool, len(c.ApprovedManual))
	for _, id := range c.ApprovedManual {
		out[id] = true
	}
	return out
}

// NewSource constructs a local-only source from configuration.
func (c *Config) NewSource() (Source, error) {
	fileSource := &FileSource{Path: c.SnapshotPath}
	snapshot, err := fileSource.Load(context.Background())
	if err != nil {
		return nil, err
	}
	switch c.Source {
	case SourceFile:
		return fileSource, nil
	case SourceMemoryFromFile:
		return NewMemorySource(snapshot), nil
	default:
		return nil, errors.New("unsupported source")
	}
}
