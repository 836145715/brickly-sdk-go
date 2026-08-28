package brickly

import (
	"context"
	"testing"
)

func TestDependencyInteractRequiresActiveCommand(t *testing.T) {
	p := New()
	dependency := testDependency(t, p)
	_, err := dependency.Interact(context.Background(), "chat", map[string]any{"n": 1})
	assertBppErrorCode(t, err, "PARENT_INVOCATION_REQUIRED")
}
