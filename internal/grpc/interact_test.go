package grpc

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"
)

type fakeInteractStream struct {
	ctx context.Context
	mu  sync.Mutex
	in  []*ClientFrame
	out []*ServerFrame
	idx int
}

func (f *fakeInteractStream) Context() context.Context { return f.ctx }

func (f *fakeInteractStream) Recv() (*ClientFrame, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.idx >= len(f.in) {
		return nil, io.EOF
	}
	frame := f.in[f.idx]
	f.idx++
	return frame, nil
}

func (f *fakeInteractStream) Send(frame *ServerFrame) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.out = append(f.out, protoCloneServerFrame(frame))
	return nil
}

func protoCloneServerFrame(frame *ServerFrame) *ServerFrame {
	copied := &ServerFrame{Header: &FrameHeader{}}
	if frame.GetHeader() != nil {
		copied.Header.Sequence = frame.GetHeader().GetSequence()
	}
	switch {
	case frame.GetOpened() != nil:
		copied.Body = &ServerFrame_Opened{Opened: &OpenedFrame{}}
	case frame.GetEvent() != nil:
		copied.Body = &ServerFrame_Event{Event: &EventFrame{Payload: frame.GetEvent().GetPayload()}}
	case frame.GetFinal() != nil:
		copied.Body = &ServerFrame_Final{Final: &FinalFrame{Result: frame.GetFinal().GetResult()}}
	}
	return copied
}

func clientOpen(commandID string, input any) *ClientFrame {
	value, err := AnyToBrickValue(input)
	if err != nil {
		panic(err)
	}
	return &ClientFrame{
		Header: &FrameHeader{Sequence: 1},
		Body:   &ClientFrame_Open{Open: &OpenFrame{CommandId: commandID, Input: value}},
	}
}

func clientEvent(sequence uint64, payload any) *ClientFrame {
	value, err := AnyToBrickValue(payload)
	if err != nil {
		panic(err)
	}
	return &ClientFrame{
		Header: &FrameHeader{Sequence: sequence},
		Body:   &ClientFrame_Event{Event: &EventFrame{Payload: value}},
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
		in:  []*ClientFrame{clientOpen("complete", map[string]any{"prompt": "诗"})},
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
		in: []*ClientFrame{
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
	if final.(map[string]any)["turns"] != int64(2) && final.(map[string]any)["turns"] != 2 {
		t.Fatalf("final %#v", final)
	}
}
