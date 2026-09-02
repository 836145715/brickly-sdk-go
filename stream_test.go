package brickly

import (
	"context"
	"reflect"
	"testing"
)

type fakeInteraction struct {
	closed bool
	events chan any
	result any
}

func (s *fakeInteraction) Send(context.Context, any) error { tPanic("call 不得 Send"); return nil }
func (s *fakeInteraction) SendLatest(context.Context, string, any) error {
	tPanic("call 不得 SendLatest")
	return nil
}
func (s *fakeInteraction) Request(context.Context, any) (any, error) {
	tPanic("call 不得 Request")
	return nil, nil
}
func (s *fakeInteraction) CloseInput(context.Context) error {
	s.closed = true
	return nil
}
func (s *fakeInteraction) End(ctx context.Context, timeoutMs ...int) (any, error) {
	return endInteraction(ctx, s, timeoutMs...)
}
func (s *fakeInteraction) Cancel(string)      {}
func (s *fakeInteraction) Events() <-chan any { return s.events }
func (s *fakeInteraction) Result() (any, error) {
	return s.result, nil
}

type fakeClient struct {
	command string
	input   any
	session *fakeInteraction
}

func (c *fakeClient) Invoke(context.Context, string, any) (any, error) {
	tPanic("call 不得走 Invoke")
	return nil, nil
}

func (c *fakeClient) Interact(_ context.Context, command string, input any, opts ...InteractOptions) (Interaction, error) {
	options, err := requireInteractOnEvent(opts)
	if err != nil {
		return nil, err
	}
	c.command = command
	c.input = input
	if c.session != nil && options.OnEvent != nil {
		for event := range c.session.events {
			options.OnEvent(event)
		}
	}
	return c.session, nil
}

func tPanic(message string) { panic(message) }

func TestInteractRequiresOnEvent(t *testing.T) {
	client := &fakeClient{session: &fakeInteraction{events: make(chan any)}}
	_, err := client.Interact(context.Background(), "chat", nil)
	if err == nil || err.Error() != "INVALID_ARGUMENT: interact 必须传入 OnEvent" {
		t.Fatalf("expected interact OnEvent required, got %#v", err)
	}
}

func TestCallRequiresOnEvent(t *testing.T) {
	client := &fakeClient{session: &fakeInteraction{events: make(chan any)}}
	_, err := Call(context.Background(), client, "ocr", map[string]any{"image": "x"})
	if err == nil {
		t.Fatal("expected OnEvent required")
	}
}

func TestCallUsesInteractThenEnd(t *testing.T) {
	events := make(chan any, 1)
	events <- map[string]any{"type": "token", "text": "诗"}
	close(events)
	session := &fakeInteraction{events: events, result: map[string]any{"text": "诗"}}
	client := &fakeClient{session: session}
	var got []any
	result, err := Call(context.Background(), client, "complete", map[string]any{"prompt": "写一首诗"}, CallOptions{
		OnEvent: func(event any) { got = append(got, event) },
	})
	if err != nil {
		t.Fatal(err)
	}
	if client.command != "complete" {
		t.Fatalf("command = %s", client.command)
	}
	if !session.closed {
		t.Fatal("expected End to close input")
	}
	if !reflect.DeepEqual(got, []any{map[string]any{"type": "token", "text": "诗"}}) {
		t.Fatalf("events = %#v", got)
	}
	if !reflect.DeepEqual(result, map[string]any{"text": "诗"}) {
		t.Fatalf("result = %#v", result)
	}
}

type publicOnlyInteraction struct{}

func (publicOnlyInteraction) Send(context.Context, any) error                 { return nil }
func (publicOnlyInteraction) SendLatest(context.Context, string, any) error   { return nil }
func (publicOnlyInteraction) Request(context.Context, any) (any, error)       { return nil, nil }
func (publicOnlyInteraction) End(context.Context, ...int) (any, error)        { return nil, nil }
func (publicOnlyInteraction) Cancel(string)                                  {}

func TestPublicInteractionHasNoResult(t *testing.T) {
	var session Interaction = publicOnlyInteraction{}
	if _, ok := any(session).(interface {
		Result() (any, error)
	}); ok {
		t.Fatal("公开 Interaction 不得要求 Result()")
	}
}
