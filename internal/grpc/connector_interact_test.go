package grpc

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/metadata"
)

type fakeConnectorStream struct {
	ctx       context.Context
	mu        sync.Mutex
	inbox     chan *ServerFrame
	sent      []*ClientFrame
	closeSend bool
}

func newFakeConnectorStream(ctx context.Context) *fakeConnectorStream {
	return &fakeConnectorStream{
		ctx:   ctx,
		inbox: make(chan *ServerFrame, 16),
	}
}

func (f *fakeConnectorStream) push(frame *ServerFrame) {
	f.inbox <- frame
}

func (f *fakeConnectorStream) Send(frame *ClientFrame) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	copied := &ClientFrame{Header: &FrameHeader{}}
	if frame.GetHeader() != nil {
		copied.Header.Sequence = frame.GetHeader().GetSequence()
		copied.Header.MessageId = append([]byte(nil), frame.GetHeader().GetMessageId()...)
		copied.Header.ReplyTo = append([]byte(nil), frame.GetHeader().GetReplyTo()...)
	}
	switch {
	case frame.GetOpen() != nil:
		copied.Body = &ClientFrame_Open{Open: frame.GetOpen()}
	case frame.GetEvent() != nil:
		copied.Body = &ClientFrame_Event{Event: frame.GetEvent()}
	case frame.GetRequest() != nil:
		copied.Body = &ClientFrame_Request{Request: frame.GetRequest()}
	case frame.GetCancelRequest() != nil:
		copied.Body = &ClientFrame_CancelRequest{CancelRequest: &CancelRequestFrame{}}
	}
	f.sent = append(f.sent, copied)
	return nil
}

func (f *fakeConnectorStream) Recv() (*ServerFrame, error) {
	select {
	case <-f.ctx.Done():
		return nil, f.ctx.Err()
	case frame, ok := <-f.inbox:
		if !ok || frame == nil {
			return nil, io.EOF
		}
		return frame, nil
	}
}

func (f *fakeConnectorStream) snapshot() []*ClientFrame {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*ClientFrame(nil), f.sent...)
}

func (f *fakeConnectorStream) CloseSend() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closeSend = true
	return nil
}

func (f *fakeConnectorStream) Context() context.Context { return f.ctx }
func (f *fakeConnectorStream) Header() (metadata.MD, error) {
	return nil, nil
}
func (f *fakeConnectorStream) Trailer() metadata.MD { return nil }
func (f *fakeConnectorStream) SendMsg(any) error    { return nil }
func (f *fakeConnectorStream) RecvMsg(any) error    { return io.EOF }

func mustValue(t *testing.T, input any) *BrickValue {
	t.Helper()
	value, err := AnyToBrickValue(input)
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func openedFrame(sequence uint64) *ServerFrame {
	return &ServerFrame{
		Header: &FrameHeader{Sequence: sequence},
		Body:   &ServerFrame_Opened{Opened: &OpenedFrame{}},
	}
}

func openTestSession(t *testing.T) (*ConnectorInteraction, *fakeConnectorStream, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	stream := newFakeConnectorStream(ctx)
	stream.push(openedFrame(1))
	session := &ConnectorInteraction{
		stream:  stream,
		cancel:  cancel,
		events:  make(chan any, 256),
		done:    make(chan struct{}),
		pending: make(map[string]chan resultOrError),
	}
	if err := session.open("echo-stream", mustValue(t, map[string]any{"via": "go"})); err != nil {
		t.Fatal(err)
	}
	go session.readLoop()
	return session, stream, cancel
}

func TestConnectorInteractOpenRequiresOpened(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream := newFakeConnectorStream(ctx)
	value, err := AnyToBrickValue(map[string]any{"n": 1})
	if err != nil {
		t.Fatal(err)
	}
	stream.push(&ServerFrame{
		Header: &FrameHeader{Sequence: 1},
		Body:   &ServerFrame_Event{Event: &EventFrame{Payload: value}},
	})
	session := &ConnectorInteraction{stream: stream, cancel: cancel}
	err = session.open("echo-stream", value)
	if err == nil || !strings.Contains(err.Error(), "opened") {
		t.Fatalf("got %v", err)
	}
}

func TestConnectorInteractRequestPairsByReplyTo(t *testing.T) {
	session, stream, cancel := openTestSession(t)
	defer cancel()
	firstCh := make(chan resultOrError, 1)
	secondCh := make(chan resultOrError, 1)
	go func() {
		result, err := session.Request(context.Background(), map[string]any{"id": "A"})
		firstCh <- resultOrError{result: result, err: err}
	}()
	firstID := waitRequestID(t, stream, "A")
	go func() {
		result, err := session.Request(context.Background(), map[string]any{"id": "B"})
		secondCh <- resultOrError{result: result, err: err}
	}()
	secondID := waitRequestID(t, stream, "B")
	stream.push(&ServerFrame{
		Header: &FrameHeader{Sequence: 2, ReplyTo: append([]byte(nil), 9)},
		Body:   &ServerFrame_Response{Response: &ResponseFrame{Outcome: &ResponseFrame_Value{Value: mustValue(t, map[string]any{"ghost": true})}}},
	})
	stream.push(&ServerFrame{
		Header: &FrameHeader{Sequence: 3, ReplyTo: secondID},
		Body:   &ServerFrame_Response{Response: &ResponseFrame{Outcome: &ResponseFrame_Value{Value: mustValue(t, map[string]any{"id": "B"})}}},
	})
	stream.push(&ServerFrame{
		Header: &FrameHeader{Sequence: 4, ReplyTo: firstID},
		Body:   &ServerFrame_Response{Response: &ResponseFrame{Outcome: &ResponseFrame_Value{Value: mustValue(t, map[string]any{"id": "A"})}}},
	})
	second := <-secondCh
	first := <-firstCh
	if second.err != nil || first.err != nil {
		t.Fatalf("request err %v %v", first.err, second.err)
	}
	if second.result.(map[string]any)["id"] != "B" || first.result.(map[string]any)["id"] != "A" {
		t.Fatalf("paired %#v %#v", first.result, second.result)
	}
}

func TestConnectorInteractRequestTimeoutWritesCancelRequest(t *testing.T) {
	session, stream, cancel := openTestSession(t)
	defer cancel()
	ctx, stop := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer stop()
	_, err := session.Request(ctx, map[string]any{"id": "slow"})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v", err)
	}
	var cancelFrame *ClientFrame
	for _, frame := range stream.snapshot() {
		if frame.GetCancelRequest() != nil {
			cancelFrame = frame
		}
	}
	if cancelFrame == nil {
		t.Fatal("expected cancel_request")
	}
	stream.push(&ServerFrame{
		Header: &FrameHeader{Sequence: 2, ReplyTo: cancelFrame.GetHeader().GetMessageId()},
		Body:   &ServerFrame_Response{Response: &ResponseFrame{Outcome: &ResponseFrame_Value{Value: mustValue(t, map[string]any{"id": "slow"})}}},
	})
	if err := session.CloseInput(context.Background()); err != nil {
		t.Fatal(err)
	}
	stream.push(&ServerFrame{
		Header: &FrameHeader{Sequence: 3},
		Body:   &ServerFrame_Final{Final: &FinalFrame{Result: mustValue(t, nil)}},
	})
	result, err := session.Result()
	if err != nil || result != nil {
		t.Fatalf("result %#v %v", result, err)
	}
}

func TestConnectorInteractResponseErrorKeepsSession(t *testing.T) {
	session, stream, cancel := openTestSession(t)
	defer cancel()
	done := make(chan resultOrError, 1)
	go func() {
		result, err := session.Request(context.Background(), map[string]any{"fail": true})
		done <- resultOrError{result: result, err: err}
	}()
	deadline := time.Now().Add(time.Second)
	var messageID []byte
	for time.Now().Before(deadline) {
		sent := stream.snapshot()
		if len(sent) >= 2 {
			messageID = sent[len(sent)-1].GetHeader().GetMessageId()
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	stream.push(&ServerFrame{
		Header: &FrameHeader{Sequence: 2, ReplyTo: messageID},
		Body: &ServerFrame_Response{Response: &ResponseFrame{Outcome: &ResponseFrame_Error{Error: &BrickError{
			Code:    "INVALID_INPUT",
			Message: "request 被拒绝",
		}}}},
	})
	outcome := <-done
	if outcome.err == nil || !strings.Contains(outcome.err.Error(), "request 被拒绝") {
		t.Fatalf("got %v", outcome.err)
	}
	if err := session.Send(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	sent := stream.snapshot()
	if sent[len(sent)-1].GetEvent() == nil {
		t.Fatal("expected event")
	}
}

func TestConnectorInteractBadSequenceCancels(t *testing.T) {
	session, stream, cancel := openTestSession(t)
	defer cancel()
	stream.push(&ServerFrame{
		Header: &FrameHeader{Sequence: 3},
		Body:   &ServerFrame_Event{Event: &EventFrame{Payload: mustValue(t, map[string]any{"n": 1})}},
	})
	_, err := session.Result()
	if err == nil || !strings.Contains(err.Error(), "sequence") {
		t.Fatalf("got %v", err)
	}
}

func TestConnectorInteractCloseInputIdempotent(t *testing.T) {
	session, stream, cancel := openTestSession(t)
	defer cancel()
	if err := session.CloseInput(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := session.CloseInput(context.Background()); err != nil {
		t.Fatal(err)
	}
	stream.mu.Lock()
	closed := stream.closeSend
	stream.mu.Unlock()
	if !closed {
		t.Fatal("expected CloseSend")
	}
	if err := session.Send(context.Background(), map[string]any{"late": true}); err == nil {
		t.Fatal("expected send to fail")
	}
	if _, err := session.Request(context.Background(), map[string]any{"late": true}); err == nil {
		t.Fatal("expected request to fail")
	}
}

func waitRequestID(t *testing.T, stream *fakeConnectorStream, id string) []byte {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		for _, frame := range stream.snapshot() {
			if frame.GetRequest() == nil {
				continue
			}
			got, _ := brickValueToAny(frame.GetRequest().GetPayload()).(map[string]any)
			if got["id"] == id {
				return frame.GetHeader().GetMessageId()
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("missing request %s in %#v", id, stream.snapshot())
	return nil
}

func TestConnectorInteractSendLatestDoesNotCoalesce(t *testing.T) {
	session, stream, cancel := openTestSession(t)
	defer cancel()
	if err := session.SendLatest(context.Background(), "preview", map[string]any{"cols": 80}); err != nil {
		t.Fatal(err)
	}
	if err := session.SendLatest(context.Background(), "preview", map[string]any{"cols": 120}); err != nil {
		t.Fatal(err)
	}
	events := 0
	for _, frame := range stream.snapshot() {
		if frame.GetEvent() != nil {
			events++
		}
	}
	if events != 2 {
		t.Fatalf("events %d", events)
	}
}
