package grpc

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	runtimev1 "github.com/836145715/brickly-sdk-go/internal/grpc/gen"
)

type fakeInteractStream struct {
	ctx   context.Context
	mu    sync.Mutex
	in    []*runtimev1.ClientFrame
	extra chan *runtimev1.ClientFrame
	out   []*runtimev1.ServerFrame
	idx   int
}

func (f *fakeInteractStream) Context() context.Context { return f.ctx }

func (f *fakeInteractStream) Recv() (*runtimev1.ClientFrame, error) {
	f.mu.Lock()
	if f.idx < len(f.in) {
		frame := f.in[f.idx]
		f.idx++
		f.mu.Unlock()
		return frame, nil
	}
	extra := f.extra
	f.mu.Unlock()
	if extra == nil {
		return nil, io.EOF
	}
	frame, ok := <-extra
	if !ok {
		return nil, io.EOF
	}
	return frame, nil
}

func (f *fakeInteractStream) Send(frame *runtimev1.ServerFrame) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.out = append(f.out, protoCloneServerFrame(frame))
	return nil
}

func protoCloneServerFrame(frame *runtimev1.ServerFrame) *runtimev1.ServerFrame {
	copied := &runtimev1.ServerFrame{Header: &runtimev1.FrameHeader{}}
	if frame.GetHeader() != nil {
		copied.Header.Sequence = frame.GetHeader().GetSequence()
		copied.Header.ReplyTo = append([]byte(nil), frame.GetHeader().GetReplyTo()...)
		copied.Header.MessageId = append([]byte(nil), frame.GetHeader().GetMessageId()...)
	}
	switch {
	case frame.GetOpened() != nil:
		copied.Body = &runtimev1.ServerFrame_Opened{Opened: &runtimev1.OpenedFrame{}}
	case frame.GetEvent() != nil:
		copied.Body = &runtimev1.ServerFrame_Event{Event: &runtimev1.EventFrame{Payload: frame.GetEvent().GetPayload()}}
	case frame.GetResponse() != nil:
		copied.Body = &runtimev1.ServerFrame_Response{Response: frame.GetResponse()}
	case frame.GetFinal() != nil:
		copied.Body = &runtimev1.ServerFrame_Final{Final: &runtimev1.FinalFrame{Result: frame.GetFinal().GetResult()}}
	}
	return copied
}

func clientOpen(commandID string, input any) *runtimev1.ClientFrame {
	value, err := AnyToBrickValue(input)
	if err != nil {
		panic(err)
	}
	return &runtimev1.ClientFrame{
		Header: &runtimev1.FrameHeader{Sequence: 1},
		Body:   &runtimev1.ClientFrame_Open{Open: &runtimev1.OpenFrame{CommandId: commandID, Input: value}},
	}
}

func clientEvent(sequence uint64, payload any) *runtimev1.ClientFrame {
	value, err := AnyToBrickValue(payload)
	if err != nil {
		panic(err)
	}
	return &runtimev1.ClientFrame{
		Header: &runtimev1.FrameHeader{Sequence: sequence},
		Body:   &runtimev1.ClientFrame_Event{Event: &runtimev1.EventFrame{Payload: value}},
	}
}

func TestDispatchInteractSendThenReturn(t *testing.T) {
	server := &commandServer{
		interact: func(commandID string, session InteractSession) (any, error) {
			if commandID != "complete" {
				t.Fatalf("command %s", commandID)
			}
			if err := session.Send(map[string]any{"type": "token", "text": "诗"}); err != nil {
				return nil, err
			}
			return map[string]any{"text": "诗"}, nil
		},
	}
	stream := &fakeInteractStream{
		ctx: context.Background(),
		in:  []*runtimev1.ClientFrame{clientOpen("complete", map[string]any{"prompt": "诗"})},
	}
	if err := server.dispatchInteract(stream); err != nil {
		t.Fatal(err)
	}
	if len(stream.out) != 3 {
		t.Fatalf("frames %#v", stream.out)
	}
	if stream.out[0].GetOpened() == nil {
		t.Fatal("expected opened")
	}
	if got := brickValueToAny(stream.out[1].GetEvent().GetPayload()); got.(map[string]any)["type"] != "token" {
		t.Fatalf("event %#v", got)
	}
	if got := brickValueToAny(stream.out[2].GetFinal().GetResult()); got.(map[string]any)["text"] != "诗" {
		t.Fatalf("final %#v", got)
	}
}

func clientRequest(sequence uint64, messageID []byte, payload any) *runtimev1.ClientFrame {
	value, err := AnyToBrickValue(payload)
	if err != nil {
		panic(err)
	}
	return &runtimev1.ClientFrame{
		Header: &runtimev1.FrameHeader{Sequence: sequence, MessageId: messageID},
		Body:   &runtimev1.ClientFrame_Request{Request: &runtimev1.RequestFrame{Payload: value}},
	}
}

func clientCancelRequest(sequence uint64, messageID []byte) *runtimev1.ClientFrame {
	return &runtimev1.ClientFrame{
		Header: &runtimev1.FrameHeader{Sequence: sequence, MessageId: messageID},
		Body:   &runtimev1.ClientFrame_CancelRequest{CancelRequest: &runtimev1.CancelRequestFrame{}},
	}
}

func messageID(b byte) []byte {
	id := make([]byte, 16)
	for i := range id {
		id[i] = b
	}
	return id
}

func waitFrame(t *testing.T, stream *fakeInteractStream, match func(*runtimev1.ServerFrame) bool) *runtimev1.ServerFrame {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		stream.mu.Lock()
		out := append([]*runtimev1.ServerFrame(nil), stream.out...)
		stream.mu.Unlock()
		for _, frame := range out {
			if match(frame) {
				return frame
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("timed out waiting for frame")
	return nil
}

func TestDispatchInteractRequestUnavailableKeepsSession(t *testing.T) {
	server := &commandServer{
		interact: func(_ string, session InteractSession) (any, error) {
			for range session.Events() {
			}
			return map[string]any{"ok": true}, nil
		},
	}
	extra := make(chan *runtimev1.ClientFrame, 4)
	stream := &fakeInteractStream{
		ctx:   context.Background(),
		in:    []*runtimev1.ClientFrame{clientOpen("assist", map[string]any{"file": "main.ts"})},
		extra: extra,
	}
	done := make(chan error, 1)
	go func() { done <- server.dispatchInteract(stream) }()
	waitFrame(t, stream, func(frame *runtimev1.ServerFrame) bool { return frame.GetOpened() != nil })
	id := messageID(1)
	extra <- clientRequest(2, id, map[string]any{"n": 1})
	resp := waitFrame(t, stream, func(frame *runtimev1.ServerFrame) bool { return frame.GetResponse() != nil })
	if resp.GetResponse().GetError().GetCode() != "REQUEST_HANDLER_UNAVAILABLE" {
		t.Fatalf("error %#v", resp.GetResponse().GetError())
	}
	close(extra)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestDispatchInteractRequestReturn(t *testing.T) {
	registered := make(chan struct{})
	server := &commandServer{
		interact: func(_ string, session InteractSession) (any, error) {
			if err := session.HandleRequests(func(req any, _ context.Context) (any, error) {
				return map[string]any{"echo": req}, nil
			}); err != nil {
				return nil, err
			}
			close(registered)
			for range session.Events() {
			}
			return map[string]any{"ok": true}, nil
		},
	}
	extra := make(chan *runtimev1.ClientFrame, 4)
	stream := &fakeInteractStream{
		ctx:   context.Background(),
		in:    []*runtimev1.ClientFrame{clientOpen("assist", map[string]any{})},
		extra: extra,
	}
	done := make(chan error, 1)
	go func() { done <- server.dispatchInteract(stream) }()
	select {
	case <-registered:
	case <-time.After(2 * time.Second):
		t.Fatal("handler not registered")
	}
	id := messageID(2)
	extra <- clientRequest(2, id, map[string]any{"id": "A"})
	resp := waitFrame(t, stream, func(frame *runtimev1.ServerFrame) bool { return frame.GetResponse() != nil })
	if got := brickValueToAny(resp.GetResponse().GetValue()).(map[string]any)["echo"].(map[string]any)["id"]; got != "A" {
		t.Fatalf("got %#v", brickValueToAny(resp.GetResponse().GetValue()))
	}
	if string(resp.GetHeader().GetReplyTo()) != string(id) {
		t.Fatal("reply_to mismatch")
	}
	close(extra)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestDispatchInteractRequestOutOfOrder(t *testing.T) {
	registered := make(chan struct{})
	releaseA := make(chan struct{})
	releaseB := make(chan struct{})
	server := &commandServer{
		interact: func(_ string, session InteractSession) (any, error) {
			if err := session.HandleRequests(func(req any, _ context.Context) (any, error) {
				id := req.(map[string]any)["id"]
				if id == "A" {
					<-releaseA
				} else {
					<-releaseB
				}
				return map[string]any{"id": id}, nil
			}); err != nil {
				return nil, err
			}
			close(registered)
			for range session.Events() {
			}
			return nil, nil
		},
	}
	extra := make(chan *runtimev1.ClientFrame, 4)
	stream := &fakeInteractStream{
		ctx:   context.Background(),
		in:    []*runtimev1.ClientFrame{clientOpen("assist", map[string]any{})},
		extra: extra,
	}
	done := make(chan error, 1)
	go func() { done <- server.dispatchInteract(stream) }()
	<-registered
	idA, idB := messageID(1), messageID(2)
	extra <- clientRequest(2, idA, map[string]any{"id": "A"})
	extra <- clientRequest(3, idB, map[string]any{"id": "B"})
	close(releaseB)
	first := waitFrame(t, stream, func(frame *runtimev1.ServerFrame) bool {
		return frame.GetResponse() != nil && string(frame.GetHeader().GetReplyTo()) == string(idB)
	})
	if brickValueToAny(first.GetResponse().GetValue()).(map[string]any)["id"] != "B" {
		t.Fatalf("first %#v", brickValueToAny(first.GetResponse().GetValue()))
	}
	close(releaseA)
	waitFrame(t, stream, func(frame *runtimev1.ServerFrame) bool {
		return frame.GetResponse() != nil && string(frame.GetHeader().GetReplyTo()) == string(idA)
	})
	close(extra)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestDispatchInteractCancelRequestKeepsSession(t *testing.T) {
	registered := make(chan struct{})
	started := make(chan struct{})
	server := &commandServer{
		interact: func(_ string, session InteractSession) (any, error) {
			if err := session.HandleRequests(func(_ any, ctx context.Context) (any, error) {
				close(started)
				<-ctx.Done()
				return nil, ctx.Err()
			}); err != nil {
				return nil, err
			}
			close(registered)
			for range session.Events() {
			}
			return map[string]any{"ok": true}, nil
		},
	}
	extra := make(chan *runtimev1.ClientFrame, 4)
	stream := &fakeInteractStream{
		ctx:   context.Background(),
		in:    []*runtimev1.ClientFrame{clientOpen("assist", map[string]any{})},
		extra: extra,
	}
	done := make(chan error, 1)
	go func() { done <- server.dispatchInteract(stream) }()
	<-registered
	id := messageID(9)
	extra <- clientRequest(2, id, map[string]any{"slow": true})
	<-started
	extra <- clientCancelRequest(3, id)
	resp := waitFrame(t, stream, func(frame *runtimev1.ServerFrame) bool { return frame.GetResponse() != nil })
	if resp.GetResponse().GetError().GetCode() != "CANCELLED" {
		t.Fatalf("got %#v", resp.GetResponse().GetError())
	}
	close(extra)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestDispatchInteractOnEventThenClosed(t *testing.T) {
	server := &commandServer{
		interact: func(_ string, session InteractSession) (any, error) {
			turns := 0
			for event := range session.Events() {
				turns++
				if err := session.Send(map[string]any{"echo": event}); err != nil {
					return nil, err
				}
			}
			return map[string]any{"turns": turns}, nil
		},
	}
	stream := &fakeInteractStream{
		ctx: context.Background(),
		in: []*runtimev1.ClientFrame{
			clientOpen("chat", map[string]any{"model": "x"}),
			clientEvent(2, map[string]any{"text": "你好"}),
			clientEvent(3, map[string]any{"text": "再简洁"}),
		},
	}
	done := make(chan error, 1)
	go func() { done <- server.dispatchInteract(stream) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("dispatch timeout")
	}
	var events int
	var final any
	for _, frame := range stream.out {
		if frame.GetEvent() != nil {
			events++
		}
		if frame.GetFinal() != nil {
			final = brickValueToAny(frame.GetFinal().GetResult())
		}
	}
	if events != 2 {
		t.Fatalf("events %d frames %#v", events, stream.out)
	}
	if final.(map[string]any)["turns"] != float64(2) {
		t.Fatalf("final %#v", final)
	}
}
