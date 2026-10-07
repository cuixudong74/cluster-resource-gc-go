package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/cuixudong74/cluster-resource-gc-go/internal/gc"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	fs := flag.NewFlagSet("cluster-resource-gc", flag.ContinueOnError)
	var (
		snapshot    = fs.String("snapshot", "", "path to local JSON snapshot")
		configPath  = fs.String("config", "", "path to local JSON config; flags override it")
		stateDir    = fs.String("state-dir", "", "directory for committed plans and recovery records")
		auditPath   = fs.String("audit", "", "append-only JSONL audit path")
		mode        = fs.String("mode", "dry-run", "dry-run or execute")
		resume      = fs.String("resume", "", "resume a committed plan ID")
		concurrency = fs.Int("concurrency", 0, "maximum concurrent deletions")
		batchSize   = fs.Int("batch-size", 0, "items per batch")
		rps         = fs.Float64("rate", 0, "maximum deletion requests per second")
		timeout     = fs.Duration("timeout", 0, "overall execution timeout")
		principal   = fs.String("principal", "", "authenticated principal name")
		roles       = fs.String("roles", "", "comma-separated roles, for example gc-admin")
		approve     = fs.String("approve", "", "comma-separated kind/namespace/name triples for manual items")
	)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *snapshot == "" && *configPath == "" {
		return errors.New("either -snapshot or -config is required")
	}

	cfg := defaultCLISnapshotConfig(*snapshot)
	if *configPath != "" {
		loaded, err := gc.LoadConfig(*configPath)
		if err != nil {
			return err
		}
		cfg = loaded
	}
	if *snapshot != "" {
		cfg.SnapshotPath = *snapshot
	}
	if *stateDir != "" {
		cfg.StateDir = *stateDir
	}
	if *auditPath != "" {
		cfg.AuditPath = *auditPath
	}
	if *resume != "" {
		cfg.ResumePlanID = *resume
	}
	if *concurrency > 0 {
		cfg.Concurrency = *concurrency
	}
	if *batchSize > 0 {
		cfg.BatchSize = *batchSize
	}
	if *rps > 0 {
		cfg.RequestsPerSecond = *rps
	}
	if *timeout > 0 {
		cfg.Timeout = gc.Duration(*timeout)
	}
	if *principal != "" {
		cfg.Principal.Name = *principal
	}
	if *roles != "" {
		cfg.Principal.Roles = splitCSV(*roles)
	}
	if *approve != "" {
		ids, err := parseObjectRefs(*approve)
		if err != nil {
			return err
		}
		cfg.ApprovedManual = ids
	}
	switch *mode {
	case "dry-run":
		cfg.DryRun = true
	case "execute":
		cfg.DryRun = false
		if cfg.Principal.Name == "" {
			return errors.New("execute requires -principal")
		}
	default:
		return fmt.Errorf("unknown mode %q", *mode)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if cfg.Timeout.Duration() > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, cfg.Timeout.Duration())
		defer cancel()
	}

	plan, result, err := gc.Run(ctx, cfg)
	if err != nil {
		return err
	}
	if err := writeJSON(os.Stdout, plan); err != nil {
		return err
	}
	if result != nil {
		fmt.Fprintf(os.Stderr, "deleted=%d alreadyDeleted=%d failed=%d denied=%d\n",
			result.Deleted, result.AlreadyDeleted, result.Failed, result.Denied)
		if result.Failed > 0 || result.Denied > 0 {
			return fmt.Errorf("collection completed with failed=%d denied=%d", result.Failed, result.Denied)
		}
	}
	return nil
}

func defaultCLISnapshotConfig(snapshot string) *gc.Config {
	return &gc.Config{
		Source:            gc.SourceMemoryFromFile,
		SnapshotPath:      snapshot,
		StateDir:          gc.DefaultStateDir,
		AuditPath:         gc.DefaultAuditPath,
		DryRun:            true,
		Concurrency:       gc.DefaultConcurrency,
		BatchSize:         gc.DefaultBatchSize,
		MaxAttempts:       gc.DefaultMaxAttempts,
		RequestsPerSecond: gc.DefaultRPS,
		Timeout:           gc.Duration(10 * time.Minute),
	}
}

func splitCSV(value string) []string {
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func parseObjectRefs(value string) ([]gc.ObjectID, error) {
	refs := splitCSV(value)
	out := make([]gc.ObjectID, 0, len(refs))
	for _, ref := range refs {
		parts := strings.Split(ref, "/")
		if len(parts) == 3 {
			out = append(out, gc.ObjectID{Kind: parts[0], Namespace: parts[1], Name: parts[2]})
			continue
		}
		if len(parts) == 2 {
			out = append(out, gc.ObjectID{Kind: parts[0], Name: parts[1]})
			continue
		}
		return nil, fmt.Errorf("invalid object reference %q, want kind/name or kind/namespace/name", ref)
	}
	return out, nil
}

func writeJSON(file *os.File, value any) error {
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	return encoder.Encode(value)
}
