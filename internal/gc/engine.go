package gc

import (
	"context"
)

// Run executes the configured local dry-run or safe collection workflow.
func Run(ctx context.Context, cfg *Config) (*Plan, *ExecuteResult, error) {
	if cfg == nil {
		return nil, nil, ErrConfigRequired
	}
	cfg.defaults()
	if err := cfg.Validate(); err != nil {
		return nil, nil, err
	}

	source, err := cfg.NewSource()
	if err != nil {
		return nil, nil, err
	}
	snapshot, err := source.Load(ctx)
	if err != nil {
		return nil, nil, err
	}

	// Snapshot time is the only evaluation clock. Using wall time here would
	// make grace/idle decisions vary for identical snapshot input.
	cfg.Policy.Now = snapshot.GeneratedAt
	analyzer := Analyzer{
		Now:           snapshot.GeneratedAt,
		IdleThreshold: cfg.Policy.IdleThreshold,
	}
	analysis, err := analyzer.Analyze(ctx, snapshot)
	if err != nil {
		return nil, nil, err
	}

	planner := &Planner{
		Policy:         cfg.Policy,
		ApprovedManual: cfg.ApprovedManualSet(),
	}
	var plan *Plan
	store := &FileCheckpointStore{Directory: cfg.StateDir}
	if cfg.ResumePlanID != "" {
		plan, err = store.LoadPlan(ctx, cfg.ResumePlanID)
		if err != nil {
			return nil, nil, err
		}
		// The snapshot must still fingerprint to the same committed plan. This
		// prevents an old recovery ID from silently applying to changed input.
		fingerprint, fingerprintErr := planner.InputFingerprint(snapshot)
		if fingerprintErr != nil {
			return nil, nil, fingerprintErr
		}
		if plan.InputFingerprint != fingerprint {
			return nil, nil, ErrPlanInputChanged
		}
	} else {
		plan, err = planner.DryRun(snapshot, analysis)
		if err != nil {
			return nil, nil, err
		}
		if err := store.SavePlan(ctx, plan); err != nil {
			return nil, nil, err
		}
	}

	if cfg.DryRun {
		return plan, nil, nil
	}

	var audit AuditSink
	if cfg.AuditPath != "" {
		audit = &FileAuditSink{Path: cfg.AuditPath}
	} else {
		audit = &MemoryAuditSink{}
	}
	executor := &Executor{
		Source:            source,
		Store:             store,
		Audit:             audit,
		Authz:             NewDefaultAuthorizer(),
		Concurrency:       cfg.Concurrency,
		BatchSize:         cfg.BatchSize,
		MaxAttempts:       cfg.MaxAttempts,
		RequestsPerSecond: cfg.RequestsPerSecond,
	}
	result, err := executor.Execute(ctx, plan, cfg.Principal)
	return plan, result, err
}

// NewDefaultAuthorizer permits only the local gc-admin delete role.
func NewDefaultAuthorizer() Authorizer {
	return &RBACAuthorizer{AllowedRoles: map[string][]string{
		AuditActionDelete: {DefaultDeleteRole},
	}}
}
