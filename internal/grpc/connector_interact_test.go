package grpc

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	runtimev1 "github.com/836145715/brickly-sdk-go/internal/grpc/gen"
	"google.golang.org/grpc/metadata"
)

type fakeConnectorStream struct {
	ctx       context.Context
	mu        sync.Mutex
	inbox     chan *runtimev1.ServerFrame
	sent      []*runtimev1.ClientFrame
	closeSend bool
}

func newFakeConnectorStream(ctx context.Context) *fakeConnectorStream {
	return &fakeConnectorStream{
		ctx:   ctx,
		inbox: make(chan *runtimev1.ServerFrame, 16),
	}
}

func (f *fakeConnectorStream) push(frame *runtimev1.ServerFrame) {
	f.inbox <- frame
}

func (f *fakeConnectorStream) Send(frame *runtimev1.ClientFrame) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	copied := &runtimev1.ClientFrame{Header: &runtimev1.FrameHeader{}}
	if frame.GetHeader() != nil {
		copied.Header.Sequence = frame.GetHeader().GetSequence()
		copied.Header.MessageId = append([]byte(nil), frame.GetHeader().GetMessageId()...)
		copied.Header.ReplyTo = append([]byte(nil), frame.GetHeader().GetReplyTo()...)
	}
	switch {
	case frame.GetOpen() != nil:
		copied.Body = &runtimev1.ClientFrame_Open{Open: frame.GetOpen()}
	case frame.GetEvent() != nil:
		copied.Body = &runtimev1.ClientFrame_Event{Event: frame.GetEvent()}
	case frame.GetRequest() != nil:
		copied.Body = &runtimev1.ClientFrame_Request{Request: frame.GetRequest()}
	case frame.GetCancelRequest() != nil:
		copied.Body = &runtimev1.ClientFrame_CancelRequest{CancelRequest: &runtimev1.CancelRequestFrame{}}
	}
	f.sent = append(f.sent, copied)
	return nil
}

func (f *fakeConnectorStream) Recv() (*runtimev1.ServerFrame, error) {
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

func (f *fakeConnectorStream) snapshot() []*runtimev1.ClientFrame {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*runtimev1.ClientFrame(nil), f.sent...)
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

func mustValue(t *testing.T, input any) *runtimev1.BrickValue {
	t.Helper()
	value, err := AnyToBrickValue(input)
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func openedFrame(sequence uint64) *runtimev1.ServerFrame {
	return &runtimev1.ServerFrame{
		Header: &runtimev1.FrameHeader{Sequence: sequence},
		Body:   &runtimev1.ServerFrame_Opened{Opened: &runtimev1.OpenedFrame{}},
	}
}

func openTestSession(t *testing.T) (*ConnectorInteraction, *fakeConnectorStream, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	stream := newFakeConnectorStream(ctx)
	stream.push(openedFrame(1))
	session := newConnectorInteraction(stream, cancel)
	if err := session.open("echo-stream", mustValue(t, map[string]any{"via": "go"})); err != nil {
		t.Fatal(err)
	}
	session.startWriter()
	go session.readLoop()
	return session, stream, cancel
}

func openPausedSession(t *testing.T) (*ConnectorInteraction, *fakeConnectorStream, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	stream := newFakeConnectorStream(ctx)
	stream.push(openedFrame(1))
	session := newConnectorInteraction(stream, cancel)
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
	stream.push(&runtimev1.ServerFrame{
		Header: &runtimev1.FrameHeader{Sequence: 1},
		Body:   &runtimev1.ServerFrame_Event{Event: &runtimev1.EventFrame{Payload: value}},
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
	stream.push(&runtimev1.ServerFrame{
		Header: &runtimev1.FrameHeader{Sequence: 2, ReplyTo: append([]byte(nil), 9)},
		Body:   &runtimev1.ServerFrame_Response{Response: &runtimev1.ResponseFrame{Outcome: &runtimev1.ResponseFrame_Value{Value: mustValue(t, map[string]any{"ghost": true})}}},
	})
	stream.push(&runtimev1.ServerFrame{
		Header: &runtimev1.FrameHeader{Sequence: 3, ReplyTo: secondID},
		Body:   &runtimev1.ServerFrame_Response{Response: &runtimev1.ResponseFrame{Outcome: &runtimev1.ResponseFrame_Value{Value: mustValue(t, map[string]any{"id": "B"})}}},
	})
	stream.push(&runtimev1.ServerFrame{
		Header: &runtimev1.FrameHeader{Sequence: 4, ReplyTo: firstID},
		Body:   &runtimev1.ServerFrame_Response{Response: &runtimev1.ResponseFrame{Outcome: &runtimev1.ResponseFrame_Value{Value: mustValue(t, map[string]any{"id": "A"})}}},
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
	var cancelFrame *runtimev1.ClientFrame
	for _, frame := range stream.snapshot() {
		if frame.GetCancelRequest() != nil {
			cancelFrame = frame
		}
	}
	if cancelFrame == nil {
		t.Fatal("expected cancel_request")
	}
	stream.push(&runtimev1.ServerFrame{
		Header: &runtimev1.FrameHeader{Sequence: 2, ReplyTo: cancelFrame.GetHeader().GetMessageId()},
		Body:   &runtimev1.ServerFrame_Response{Response: &runtimev1.ResponseFrame{Outcome: &runtimev1.ResponseFrame_Value{Value: mustValue(t, map[string]any{"id": "slow"})}}},
	})
	if err := session.CloseInput(context.Background()); err != nil {
		t.Fatal(err)
	}
	stream.push(&runtimev1.ServerFrame{
		Header: &runtimev1.FrameHeader{Sequence: 3},
		Body:   &runtimev1.ServerFrame_Final{Final: &runtimev1.FinalFrame{Result: mustValue(t, nil)}},
	})
	result, err := session.Result()
	if err != nil || result != nil {
		t.Fatalf("result %#v %v", result, err)
	}
}

func TestConnectorInteractCancelCtxOnlySettlesThatRequest(t *testing.T) {
	session, stream, cancel := openTestSession(t)
	defer cancel()
	reqCtx, stop := context.WithCancel(context.Background())
	slowCh := make(chan resultOrError, 1)
	keepCh := make(chan resultOrError, 1)
	go func() {
		result, err := session.Request(reqCtx, map[string]any{"id": "slow"})
		slowCh <- resultOrError{result: result, err: err}
	}()
	slowID := waitRequestID(t, stream, "slow")
	go func() {
		result, err := session.Request(context.Background(), map[string]any{"id": "keep"})
		keepCh <- resultOrError{result: result, err: err}
	}()
	keepID := waitRequestID(t, stream, "keep")
	stop()
	stop()
	slow := <-slowCh
	if !errors.Is(slow.err, context.Canceled) {
		t.Fatalf("got %v", slow.err)
	}
	cancelFrame := waitCancelRequest(t, stream)
	if string(cancelFrame.GetHeader().GetMessageId()) != string(slowID) {
		t.Fatalf("cancel message_id=%x want %x", cancelFrame.GetHeader().GetMessageId(), slowID)
	}
	if n := len(framesOf(stream, "cancel_request")); n != 1 {
		t.Fatalf("cancel_request count %d", n)
	}
	stream.push(&runtimev1.ServerFrame{
		Header: &runtimev1.FrameHeader{Sequence: 2, ReplyTo: slowID},
		Body:   &runtimev1.ServerFrame_Response{Response: &runtimev1.ResponseFrame{Outcome: &runtimev1.ResponseFrame_Value{Value: mustValue(t, map[string]any{"id": "slow"})}}},
	})
	stream.push(&runtimev1.ServerFrame{
		Header: &runtimev1.FrameHeader{Sequence: 3, ReplyTo: keepID},
		Body:   &runtimev1.ServerFrame_Response{Response: &runtimev1.ResponseFrame{Outcome: &runtimev1.ResponseFrame_Value{Value: mustValue(t, map[string]any{"id": "keep"})}}},
	})
	keep := <-keepCh
	if keep.err != nil || keep.result.(map[string]any)["id"] != "keep" {
		t.Fatalf("keep %#v %v", keep.result, keep.err)
	}
	if err := session.Send(context.Background(), map[string]any{"after": true}); err != nil {
		t.Fatal(err)
	}
	if framesOf(stream, "event") == nil {
		t.Fatal("expected event after cancel")
	}
	nextCh := make(chan resultOrError, 1)
	go func() {
		result, err := session.Request(context.Background(), map[string]any{"id": "next"})
		nextCh <- resultOrError{result: result, err: err}
	}()
	nextID := waitRequestID(t, stream, "next")
	stream.push(&runtimev1.ServerFrame{
		Header: &runtimev1.FrameHeader{Sequence: 4, ReplyTo: nextID},
		Body:   &runtimev1.ServerFrame_Response{Response: &runtimev1.ResponseFrame{Outcome: &runtimev1.ResponseFrame_Value{Value: mustValue(t, map[string]any{"id": "next"})}}},
	})
	next := <-nextCh
	if next.err != nil || next.result.(map[string]any)["id"] != "next" {
		t.Fatalf("next %#v %v", next.result, next.err)
	}
	endCh := make(chan resultOrError, 1)
	go func() {
		result, err := session.End(context.Background())
		endCh <- resultOrError{result: result, err: err}
	}()
	stream.push(&runtimev1.ServerFrame{
		Header: &runtimev1.FrameHeader{Sequence: 5},
		Body:   &runtimev1.ServerFrame_Final{Final: &runtimev1.FinalFrame{Result: mustValue(t, map[string]any{"ok": true})}},
	})
	ended := waitOutcome(t, endCh)
	if ended.err != nil || ended.result.(map[string]any)["ok"] != true {
		t.Fatalf("end %#v %v", ended.result, ended.err)
	}
}

func TestConnectorInteractCancelCtxDropsUnsentRequest(t *testing.T) {
	session, stream, cancel := openPausedSession(t)
	defer cancel()
	reqCtx, stop := context.WithCancel(context.Background())
	done := make(chan resultOrError, 1)
	go func() {
		result, err := session.Request(reqCtx, map[string]any{"id": "slow"})
		done <- resultOrError{result: result, err: err}
	}()
	waitUntilQueued(t, session)
	stop()
	outcome := waitOutcome(t, done)
	if !errors.Is(outcome.err, context.Canceled) {
		t.Fatalf("got %v", outcome.err)
	}
	session.startWriter()
	if err := session.Send(context.Background(), map[string]any{"after": true}); err != nil {
		t.Fatal(err)
	}
	waitFrameKind(t, stream, "event")
	if framesOf(stream, "request") != nil {
		t.Fatalf("expected no request, got %#v", stream.snapshot())
	}
	if framesOf(stream, "cancel_request") != nil {
		t.Fatalf("expected no cancel_request, got %#v", stream.snapshot())
	}
}

func TestConnectorInteractAlreadyCanceledCtxWritesNothing(t *testing.T) {
	session, stream, cancel := openTestSession(t)
	defer cancel()
	ctx, stop := context.WithCancel(context.Background())
	stop()
	_, err := session.Request(ctx, map[string]any{"id": "slow"})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
	if framesOf(stream, "request") != nil || framesOf(stream, "cancel_request") != nil {
		t.Fatalf("expected no request/cancel, got %#v", stream.snapshot())
	}
}

func TestConnectorInteractEndSettlesSentRequestAndWritesCancel(t *testing.T) {
	session, stream, cancel := openTestSession(t)
	defer cancel()
	reqDone := make(chan resultOrError, 1)
	go func() {
		result, err := session.Request(context.Background(), map[string]any{"id": "slow"})
		reqDone <- resultOrError{result: result, err: err}
	}()
	messageID := waitRequestID(t, stream, "slow")
	endDone := make(chan resultOrError, 1)
	go func() {
		result, err := session.End(context.Background())
		endDone <- resultOrError{result: result, err: err}
	}()
	cancelFrame := waitCancelRequest(t, stream)
	if string(cancelFrame.GetHeader().GetMessageId()) != string(messageID) {
		t.Fatalf("cancel message_id=%x want %x", cancelFrame.GetHeader().GetMessageId(), messageID)
	}
	assertSessionClosed(t, waitOutcome(t, reqDone).err)
	stream.push(&runtimev1.ServerFrame{
		Header: &runtimev1.FrameHeader{Sequence: 2},
		Body:   &runtimev1.ServerFrame_Final{Final: &runtimev1.FinalFrame{Result: mustValue(t, map[string]any{"ok": true})}},
	})
	ended := waitOutcome(t, endDone)
	if ended.err != nil || ended.result.(map[string]any)["ok"] != true {
		t.Fatalf("end %#v %v", ended.result, ended.err)
	}
}

func TestConnectorInteractEndDropsUnsentRequest(t *testing.T) {
	session, stream, cancel := openPausedSession(t)
	defer cancel()
	reqDone := make(chan resultOrError, 1)
	go func() {
		result, err := session.Request(context.Background(), map[string]any{"id": "slow"})
		reqDone <- resultOrError{result: result, err: err}
	}()
	waitUntilQueued(t, session)
	endDone := make(chan resultOrError, 1)
	go func() {
		result, err := session.End(context.Background())
		endDone <- resultOrError{result: result, err: err}
	}()
	assertSessionClosed(t, waitOutcome(t, reqDone).err)
	if framesOf(stream, "request") != nil || framesOf(stream, "cancel_request") != nil {
		t.Fatalf("expected no request/cancel, got %#v", stream.snapshot())
	}
	stream.push(&runtimev1.ServerFrame{
		Header: &runtimev1.FrameHeader{Sequence: 2},
		Body:   &runtimev1.ServerFrame_Final{Final: &runtimev1.FinalFrame{Result: mustValue(t, nil)}},
	})
	ended := waitOutcome(t, endDone)
	if ended.err != nil || ended.result != nil {
		t.Fatalf("end %#v %v", ended.result, ended.err)
	}
}

func TestConnectorInteractSessionCancelWritesCancelForSent(t *testing.T) {
	session, stream, cancel := openTestSession(t)
	defer cancel()
	reqDone := make(chan resultOrError, 1)
	go func() {
		result, err := session.Request(context.Background(), map[string]any{"id": "pending"})
		reqDone <- resultOrError{result: result, err: err}
	}()
	messageID := waitRequestID(t, stream, "pending")
	session.Cancel("user")
	cancelFrame := waitCancelRequest(t, stream)
	if string(cancelFrame.GetHeader().GetMessageId()) != string(messageID) {
		t.Fatalf("cancel message_id=%x want %x", cancelFrame.GetHeader().GetMessageId(), messageID)
	}
	assertSessionClosed(t, waitOutcome(t, reqDone).err)
}

func TestConnectorInteractFinalSettlesPendingWithoutCancel(t *testing.T) {
	session, stream, cancel := openTestSession(t)
	defer cancel()
	reqDone := make(chan resultOrError, 1)
	go func() {
		result, err := session.Request(context.Background(), map[string]any{"id": "slow"})
		reqDone <- resultOrError{result: result, err: err}
	}()
	_ = waitRequestID(t, stream, "slow")
	stream.push(&runtimev1.ServerFrame{
		Header: &runtimev1.FrameHeader{Sequence: 2},
		Body:   &runtimev1.ServerFrame_Final{Final: &runtimev1.FinalFrame{Result: mustValue(t, map[string]any{"ok": true})}},
	})
	assertSessionClosed(t, waitOutcome(t, reqDone).err)
	time.Sleep(20 * time.Millisecond)
	if framesOf(stream, "cancel_request") != nil {
		t.Fatalf("expected no cancel_request, got %#v", stream.snapshot())
	}
	result, err := session.End(context.Background())
	if err != nil || result.(map[string]any)["ok"] != true {
		t.Fatalf("end %#v %v", result, err)
	}
}

func TestConnectorInteractEndCleanupIsIdempotent(t *testing.T) {
	session, stream, cancel := openTestSession(t)
	defer cancel()
	reqDone := make(chan resultOrError, 1)
	go func() {
		result, err := session.Request(context.Background(), map[string]any{"id": "slow"})
		reqDone <- resultOrError{result: result, err: err}
	}()
	_ = waitRequestID(t, stream, "slow")
	first := make(chan resultOrError, 1)
	second := make(chan resultOrError, 1)
	go func() {
		result, err := session.End(context.Background())
		first <- resultOrError{result: result, err: err}
	}()
	go func() {
		result, err := session.End(context.Background())
		second <- resultOrError{result: result, err: err}
	}()
	_ = waitCancelRequest(t, stream)
	time.Sleep(20 * time.Millisecond)
	if n := len(framesOf(stream, "cancel_request")); n != 1 {
		t.Fatalf("cancel_request count %d", n)
	}
	assertSessionClosed(t, waitOutcome(t, reqDone).err)
	stream.push(&runtimev1.ServerFrame{
		Header: &runtimev1.FrameHeader{Sequence: 2},
		Body:   &runtimev1.ServerFrame_Final{Final: &runtimev1.FinalFrame{Result: mustValue(t, nil)}},
	})
	if err := waitOutcome(t, first).err; err != nil {
		t.Fatal(err)
	}
	if err := waitOutcome(t, second).err; err != nil {
		t.Fatal(err)
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
	stream.push(&runtimev1.ServerFrame{
		Header: &runtimev1.FrameHeader{Sequence: 2, ReplyTo: messageID},
		Body: &runtimev1.ServerFrame_Response{Response: &runtimev1.ResponseFrame{Outcome: &runtimev1.ResponseFrame_Error{Error: &runtimev1.BrickError{
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
	stream.push(&runtimev1.ServerFrame{
		Header: &runtimev1.FrameHeader{Sequence: 3},
		Body:   &runtimev1.ServerFrame_Event{Event: &runtimev1.EventFrame{Payload: mustValue(t, map[string]any{"n": 1})}},
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

func assertSessionClosed(t *testing.T, err error) {
	t.Helper()
	if err == nil || !strings.Contains(err.Error(), "SESSION_CLOSED") {
		t.Fatalf("expected SESSION_CLOSED, got %v", err)
	}
}

func waitOutcome(t *testing.T, ch <-chan resultOrError) resultOrError {
	t.Helper()
	select {
	case outcome := <-ch:
		return outcome
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for request/end")
		return resultOrError{}
	}
}

func waitUntilQueued(t *testing.T, session *ConnectorInteraction) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		session.mu.Lock()
		n := len(session.outq)
		session.mu.Unlock()
		if n > 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("request 未进入发送队列")
}

func waitCancelRequest(t *testing.T, stream *fakeConnectorStream) *runtimev1.ClientFrame {
	t.Helper()
	return waitFrameKind(t, stream, "cancel_request")
}

func waitFrameKind(t *testing.T, stream *fakeConnectorStream, kind string) *runtimev1.ClientFrame {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if frames := framesOf(stream, kind); len(frames) > 0 {
			return frames[0]
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("missing %s in %#v", kind, stream.snapshot())
	return nil
}

func framesOf(stream *fakeConnectorStream, kind string) []*runtimev1.ClientFrame {
	var out []*runtimev1.ClientFrame
	for _, frame := range stream.snapshot() {
		switch kind {
		case "request":
			if frame.GetRequest() != nil {
				out = append(out, frame)
			}
		case "cancel_request":
			if frame.GetCancelRequest() != nil {
				out = append(out, frame)
			}
		case "event":
			if frame.GetEvent() != nil {
				out = append(out, frame)
			}
		}
	}
	return out
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

func TestConnectorInteractSendLatestCoalescesUnsent(t *testing.T) {
	session, stream, cancel := openPausedSession(t)
	defer cancel()
	if err := session.SendLatest(context.Background(), "preview", map[string]any{"cols": 80}); err != nil {
		t.Fatal(err)
	}
	if err := session.SendLatest(context.Background(), "preview", map[string]any{"cols": 120}); err != nil {
		t.Fatal(err)
	}
	session.startWriter()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		events := 0
		var last any
		for _, frame := range stream.snapshot() {
			if frame.GetEvent() != nil {
				events++
				last = brickValueToAny(frame.GetEvent().GetPayload())
			}
		}
		if events == 1 {
			if last.(map[string]any)["cols"] != float64(120) {
				t.Fatalf("last %#v", last)
			}
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("events %#v", stream.snapshot())
}
