package gc

import (
	"context"
	"testing"
)

func TestRBACAuthorizer(t *testing.T) {
	authorizer := NewDefaultAuthorizer()
	req := func(principal Principal) AuthorizationRequest {
		return AuthorizationRequest{Principal: principal, Action: AuditActionDelete, PlanID: "plan-test"}
	}
	decision, err := authorizer.Authorize(context.Background(), req(Principal{}))
	if err != nil {
		t.Fatal(err)
	}
	if decision.Allowed {
		t.Fatal("anonymous principal must be denied")
	}
	decision, err = authorizer.Authorize(context.Background(), req(Principal{Name: "user", Roles: []string{"viewer"}}))
	if err != nil {
		t.Fatal(err)
	}
	if decision.Allowed {
		t.Fatal("viewer must not delete")
	}
	decision, err = authorizer.Authorize(context.Background(), req(Principal{Name: "admin", Roles: []string{DefaultDeleteRole}}))
	if err != nil {
		t.Fatal(err)
	}
	if !decision.Allowed {
		t.Fatalf("gc-admin must be allowed: %s", decision.Reason)
	}
}
