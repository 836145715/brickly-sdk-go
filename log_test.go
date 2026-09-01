package brickly

import "testing"

func TestInfoDoesNotPanicWhenPlatformDisconnected(t *testing.T) {
	p := New()
	p.Debug("dbg", map[string]any{"ok": true})
	p.Info("ready", nil)
	p.Warn("warn", nil)
	p.Error("boom", errString("x"), map[string]any{"id": 1})
}

func TestCommandContextLogDoesNotPanicWhenPlatformDisconnected(t *testing.T) {
	p := New()
	ctx := newCommandContext(p, "req-1", "echo", CommandInvocationContext{Source: "unknown"}, nil, nil)
	ctx.Debug("dbg", nil)
	ctx.Info("ready", map[string]any{"ok": true})
	ctx.Warn("warn", nil)
	ctx.Error("boom", errString("x"), nil)
}

type errString string

func (e errString) Error() string { return string(e) }
