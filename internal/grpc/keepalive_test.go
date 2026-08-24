package grpc

import "testing"

func TestRuntimeKeepaliveAllowsHostPings(t *testing.T) {
	if RuntimeKeepaliveMinTime >= HostKeepaliveTime {
		t.Fatalf("Runtime MinTime %s 必须低于 Host keepalive %s，否则空闲 interact 会被 GOAWAY", RuntimeKeepaliveMinTime, HostKeepaliveTime)
	}
	policy := runtimeKeepaliveEnforcement()
	if policy.MinTime != RuntimeKeepaliveMinTime {
		t.Fatalf("EnforcementPolicy.MinTime = %s", policy.MinTime)
	}
	if !policy.PermitWithoutStream {
		t.Fatal("必须允许无 RPC 时的 Host ping，owned 空闲窗口仍要保活")
	}
}
