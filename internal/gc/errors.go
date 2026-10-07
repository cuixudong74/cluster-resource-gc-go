package gc

import "errors"

// Sentinel errors describe stable operational failures.
var (
	ErrConfigRequired   = errors.New("config is required")
	ErrPlanInputChanged = errors.New("snapshot changed since plan was committed")
)
