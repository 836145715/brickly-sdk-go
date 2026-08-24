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

func (s *fakeInteraction) Send(context.Context, any) error            { tPanic("stream 不得 Send"); return nil }
func (s *fakeInteraction) SendLatest(context.Context, string, any) error {
	tPanic("stream 不得 SendLatest")
	return nil
}
func (s *fakeInteraction) Request(context.Context, any) (any, error) {
	tPanic("stream 不得 Request")
	return nil, nil
}
func (s *fakeInteraction) CloseInput(context.Context) error {
	s.closed = true
	return nil
}
func (s *fakeInteraction) Cancel(string)           {}
func (s *fakeInteraction) Events() <-chan any      { return s.events }
func (s *fakeInteraction) Result() (any, error) { return s.result, nil }

type callClient struct {
	invokeCommand string
	invokeResult  any
}

func (c *callClient) Invoke(_ context.Context, command string, _ any) (any, error) {
	c.invokeCommand = command
	return c.invokeResult, nil
}

func (c *callClient) Interact(context.Context, string, any) (Interaction, error) {
	tPanic("无 OnEvent 不得走 Interact")
	return nil, nil
}

type fakeClient struct {
	command string
	input   any
	session *fakeInteraction
}

func (c *fakeClient) Invoke(context.Context, string, any) (any, error) {
	tPanic("stream 不得走 Invoke")
	return nil, nil
}

func (c *fakeClient) Interact(_ context.Context, command string, input any) (Interaction, error) {
	c.command = command
	c.input = input
	return c.session, nil
}

func tPanic(message string) { panic(message) }

func TestCallWithoutOnEventUsesInvoke(t *testing.T) {
	client := &callClient{invokeResult: map[string]any{"text": "HI"}}
	got, err := Call(context.Background(), client, "uppercase", map[string]any{"text": "hi"})
	if err != nil {
		t.Fatal(err)
	}
	if client.invokeCommand != "uppercase" {
		t.Fatalf("command = %s", client.invokeCommand)
	}
	if !reflect.DeepEqual(got, map[string]any{"text": "HI"}) {
		t.Fatalf("result = %#v", got)
	}
}

func TestCallWithOnEventUsesInteract(t *testing.T) {
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
		t.Fatal("expected CloseInput")
	}
	if !reflect.DeepEqual(got, []any{map[string]any{"type": "token", "text": "诗"}}) {
		t.Fatalf("events = %#v", got)
	}
	if !reflect.DeepEqual(result, map[string]any{"text": "诗"}) {
		t.Fatalf("result = %#v", result)
	}
}

func TestStreamIsInteractPlusCloseInput(t *testing.T) {
	events := make(chan any, 2)
	events <- "a"
	events <- "b"
	close(events)
	session := &fakeInteraction{events: events}
	client := &fakeClient{session: session}

	got, err := Stream(context.Background(), client, "echo-stream", map[string]any{"n": 1})
	if err != nil {
		t.Fatal(err)
	}
	if client.command != "echo-stream" {
		t.Fatalf("command = %s", client.command)
	}
	if !session.closed {
		t.Fatal("expected CloseInput")
	}
	var collected []any
	for event := range got {
		collected = append(collected, event)
	}
	if !reflect.DeepEqual(collected, []any{"a", "b"}) {
		t.Fatalf("events = %#v", collected)
	}
}
