package brickly

import (
	"testing"
	"time"
)

func TestUnarySendAndOnEventRejected(t *testing.T) {
	stream := unaryCommandStream{}
	if err := stream.Send(map[string]any{"n": 1}); err == nil {
		t.Fatal("expected send error")
	}
	if err := stream.OnEvent(func(any) {}); err == nil {
		t.Fatal("expected onEvent error")
	}
	select {
	case <-stream.Closed():
	default:
		t.Fatal("unary closed should already be done")
	}
}

func TestInteractSendAndOnEvent(t *testing.T) {
	incoming := make(chan any, 2)
	var sent []any
	stream := bindInteractStream(func(event any) error {
		sent = append(sent, event)
		return nil
	}, incoming)

	incoming <- map[string]any{"text": "你好"}
	incoming <- map[string]any{"text": "再简洁"}
	close(incoming)

	var got []any
	if err := stream.OnEvent(func(event any) {
		got = append(got, event)
		_ = stream.Send(map[string]any{"echo": event})
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-stream.Closed():
	case <-time.After(time.Second):
		t.Fatal("closed timeout")
	}
	if len(got) != 2 {
		t.Fatalf("got %d events", len(got))
	}
	if len(sent) != 2 {
		t.Fatalf("sent %d events", len(sent))
	}
}

func TestCommandContextUnarySend(t *testing.T) {
	p := New()
	ctx := newCommandContext(p, "req", "uppercase", CommandInvocationContext{Source: "unknown"}, nil, nil)
	if err := ctx.Send("x"); err == nil {
		t.Fatal("expected PROTOCOL_ERROR")
	}
}

func TestCommandContextSendThenReturn(t *testing.T) {
	p := New()
	ctx := newCommandContext(p, "req", "complete", CommandInvocationContext{Source: "unknown"}, nil, nil)
	incoming := make(chan any)
	var sent []any
	ctx.stream = bindInteractStream(func(event any) error {
		sent = append(sent, event)
		return nil
	}, incoming)
	if err := ctx.Send(map[string]any{"type": "token", "text": "诗"}); err != nil {
		t.Fatal(err)
	}
	close(incoming)
	select {
	case <-ctx.Closed():
	case <-time.After(time.Second):
		t.Fatal("closed timeout")
	}
	if len(sent) != 1 {
		t.Fatalf("sent %#v", sent)
	}
}

func TestCommandContextOnEventThenClosed(t *testing.T) {
	p := New()
	ctx := newCommandContext(p, "req", "chat", CommandInvocationContext{Source: "unknown"}, nil, nil)
	incoming := make(chan any, 2)
	var sent []any
	ctx.stream = bindInteractStream(func(event any) error {
		sent = append(sent, event)
		return nil
	}, incoming)
	incoming <- map[string]any{"text": "你好"}
	incoming <- map[string]any{"text": "再简洁"}
	close(incoming)
	if err := ctx.OnEvent(func(event any) {
		_ = ctx.Send(map[string]any{"echo": event})
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ctx.Closed():
	case <-time.After(time.Second):
		t.Fatal("closed timeout")
	}
	if len(sent) != 2 {
		t.Fatalf("sent %#v", sent)
	}
}
