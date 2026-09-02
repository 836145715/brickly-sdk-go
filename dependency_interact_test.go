package brickly

import (
	"context"
	"strings"
	"testing"
)

func TestDependencyInteractOutsideCommandRequiresHost(t *testing.T) {
	p := New()
	dependency := testDependency(t, p)
	_, err := dependency.Interact(context.Background(), "chat", map[string]any{"n": 1}, InteractOptions{OnEvent: func(any) {}})
	assertBppErrorCode(t, err, "PROTOCOL_ERROR")
	_, err = dependency.Call(context.Background(), "chat", map[string]any{"n": 1}, CallOptions{
		OnEvent: func(any) {},
	})
	assertBppErrorCode(t, err, "PROTOCOL_ERROR")
}

func TestDependencyStartOutsideCommandRequiresParent(t *testing.T) {
	p := New()
	dependency := testDependency(t, p)
	_, err := dependency.Start()
	assertBppErrorCode(t, err, "PARENT_INVOCATION_REQUIRED")
	if err == nil || !strings.Contains(err.Error(), "invoke") {
		t.Fatalf("expected start error to mention invoke hop, got %#v", err)
	}
}

func TestDependencyStartInEventScopeRejected(t *testing.T) {
	p := New()
	p.setCurrentRequestID("evt-1")
	defer p.setCurrentRequestID("")
	dependency := testDependency(t, p)
	_, err := dependency.Start()
	assertBppErrorCode(t, err, "PARENT_INVOCATION_REQUIRED")
}

func TestDependencyStartInCommandWithoutHostIsProtocol(t *testing.T) {
	p := New()
	p.enterCommand("cmd-1", nil)
	defer p.leaveCommand()
	dependency := testDependency(t, p)
	_, err := dependency.Start()
	assertBppErrorCode(t, err, "PROTOCOL_ERROR")
}

func TestRuntimeInteractAndCallOutsideCommandRequireHost(t *testing.T) {
	p := New()
	_, err := p.Interact(context.Background(), "preview", nil, InteractOptions{OnEvent: func(any) {}})
	assertBppErrorCode(t, err, "PROTOCOL_ERROR")
	_, err = p.Call(context.Background(), "preview", nil, CallOptions{OnEvent: func(any) {}})
	assertBppErrorCode(t, err, "PROTOCOL_ERROR")
	if _, ok := any(p).(interface{ InvokeSelf(string, any) (any, error) }); ok {
		t.Fatal("Runtime must not expose InvokeSelf")
	}
}

func assertInteractRequiresOnEvent(t *testing.T, err error) {
	t.Helper()
	if err == nil || err.Error() != "INVALID_ARGUMENT: interact 必须传入 OnEvent" {
		t.Fatalf("expected interact OnEvent required, got %#v", err)
	}
}

func TestRuntimeInteractRequiresOnEventBeforeHost(t *testing.T) {
	p := New()
	_, err := p.Interact(context.Background(), "preview", nil)
	assertInteractRequiresOnEvent(t, err)
}

func TestDependencyInteractRequiresOnEventBeforeHost(t *testing.T) {
	p := New()
	dependency := testDependency(t, p)
	_, err := dependency.Interact(context.Background(), "chat", nil)
	assertInteractRequiresOnEvent(t, err)
}

func TestStartedHandleInteractRequiresOnEvent(t *testing.T) {
	handle := &StartedToolHandle{
		runtime:      New(),
		ref:          testTargetRef,
		handleID:     "h1",
		invocationID: "inv-1",
	}
	_, err := handle.Interact(context.Background(), "chat", nil)
	assertInteractRequiresOnEvent(t, err)
}
