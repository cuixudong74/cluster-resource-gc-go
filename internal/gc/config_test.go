package gc

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoadConfigAppliesDefaultsAndParsesDurations(t *testing.T) {
	dir := t.TempDir()
	snapshotPath := filepath.Join(dir, "snapshot.json")
	if err := os.WriteFile(snapshotPath, []byte(`{"objects":[]}`), 0o644); err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(dir, "config.json")
	content := `{
		"snapshotPath":"` + snapshotPath + `",
		"timeout":"2m",
		"policy":{"gracePeriod":"1h","idleThreshold":"24h"},
		"principal":{"name":"bot","roles":["gc-admin"]}
	}`
	if err := os.WriteFile(configPath, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Timeout.Duration() != 2*time.Minute || cfg.Policy.GracePeriod.Duration() != time.Hour || cfg.Policy.IdleThreshold.Duration() != 24*time.Hour {
		t.Fatalf("durations not parsed: %+v", cfg)
	}
	if cfg.Source != SourceMemoryFromFile || cfg.Concurrency != DefaultConcurrency || cfg.Policy.ProtectedLabel == "" {
		t.Fatalf("defaults not applied: %+v", cfg)
	}
	source, err := cfg.NewSource()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := source.(*MemorySource); !ok {
		t.Fatalf("expected simulated memory source, got %T", source)
	}
}

func TestLoadConfigRejectsUnknownSource(t *testing.T) {
	dir := t.TempDir()
	snapshotPath := filepath.Join(dir, "snapshot.json")
	if err := os.WriteFile(snapshotPath, []byte(`{"objects":[]}`), 0o644); err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(dir, "config.json")
	content := `{"source":"real-cluster","snapshotPath":"` + snapshotPath + `"}`
	if err := os.WriteFile(configPath, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadConfig(configPath); err == nil {
		t.Fatal("expected unknown source to be rejected")
	}
}
