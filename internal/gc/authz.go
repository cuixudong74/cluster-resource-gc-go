package gc

import (
	"context"
	"strings"
)

// Principal is the authenticated caller.
type Principal struct {
	Name  string   `json:"name"`
	Roles []string `json:"roles"`
}

// AuthorizationRequest describes a requested operation.
type AuthorizationRequest struct {
	Principal Principal
	Action    string
	PlanID    string
	ID        ObjectID
}

// AuthorizationDecision carries an allow/deny decision and reason.
type AuthorizationDecision struct {
	Allowed bool
	Reason  string
}

// Authorizer authorizes mutations before they reach the source.
type Authorizer interface {
	Authorize(context.Context, AuthorizationRequest) (AuthorizationDecision, error)
}

// RBACAuthorizer is a small role-based authorizer.
type RBACAuthorizer struct {
	// AllowedRoles maps an action to roles permitted for that action.
	// A wildcard role grants every configured action.
	AllowedRoles map[string][]string
}

// Authorize denies anonymous callers and callers without an allowed role.
func (a *RBACAuthorizer) Authorize(_ context.Context, req AuthorizationRequest) (AuthorizationDecision, error) {
	if strings.TrimSpace(req.Principal.Name) == "" {
		return AuthorizationDecision{Allowed: false, Reason: "anonymous principal is not authorized"}, nil
	}
	allowedRoles := a.AllowedRoles[req.Action]
	for _, role := range req.Principal.Roles {
		if role == "*" || containsString(allowedRoles, role) {
			return AuthorizationDecision{Allowed: true, Reason: "role " + role + " permitted for " + req.Action}, nil
		}
	}
	return AuthorizationDecision{Allowed: false, Reason: "principal has no role permitted for " + req.Action}, nil
}
