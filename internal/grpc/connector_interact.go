package grpc

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"sync"
	"time"

	runtimev1 "github.com/836145715/brickly-sdk-go/internal/grpc/gen"
	"google.golang.org/grpc/metadata"
)

type interactStream interface {
	Send(*runtimev1.ClientFrame) error
	Recv() (*runtimev1.ServerFrame, error)
	CloseSend() error
}

type requestLifecycle string

const (
	requestQueued    requestLifecycle = "queued"
	requestSent      requestLifecycle = "sent"
	requestSettled   requestLifecycle = "settled"
	requestCancelled requestLifecycle = "cancelled"
)

type outboundItem struct {
	key   string
	frame *runtimev1.ClientFrame
	done  chan error
}

type pendingRequest struct {
	state     requestLifecycle
	messageID []byte
	reply     chan resultOrError
}

var errSessionClosed = fmt.Errorf("SESSION_CLOSED: interaction 已取消")

// ConnectorInteraction 是 Runtime → Host Connector.Interact 的客户端会话。
type ConnectorInteraction struct {
	stream        interactStream
	cancel        context.CancelFunc
	mu            sync.Mutex
	cond          *sync.Cond
	outbound      uint64
	inbound       uint64
	events        chan any
	done          chan struct{}
	result        any
	err           error
	pending       map[string]*pendingRequest
	closed        bool
	peerFinal     bool
	inputClosed   bool
	outq          []outboundItem
	writerStop    bool
	writerOnce    sync.Once
	writerDone    chan struct{}
	writerStarted bool
}

func (c *HostPlatformClient) Interact(ctx context.Context, brickID, commandID string, input any, invocationID string, intent ...string) (*ConnectorInteraction, error) {
	return c.interact(ctx, brickID, commandID, input, invocationID, "", firstIntent(intent))
}

func (c *HostPlatformClient) InteractOnHandle(ctx context.Context, brickID, commandID string, input any, invocationID, handleID string, intent ...string) (*ConnectorInteraction, error) {
	return c.interact(ctx, brickID, commandID, input, invocationID, handleID, firstIntent(intent))
}

func (c *HostPlatformClient) PlatformInteract(ctx context.Context, commandID string, input any, invocationID string, intent ...string) (*ConnectorInteraction, error) {
	normalized, err := jsonInput(input)
	if err != nil {
		return nil, err
	}
	value, err := AnyToBrickValue(normalized)
	if err != nil {
		return nil, err
	}
	callCtx := c.withToken(ctx)
	if invocationID != "" {
		callCtx = metadata.AppendToOutgoingContext(callCtx, InvocationIdMD, invocationID)
	}
	callCtx = appendCommandIntent(callCtx, firstIntent(intent))
	callCtx, cancel := context.WithCancel(callCtx)
	stream, err := c.platform.Interact(callCtx)
	if err != nil {
		cancel()
		return nil, err
	}
	session := newConnectorInteraction(stream, cancel)
	if err := session.open(commandID, value); err != nil {
		cancel()
		_ = stream.CloseSend()
		return nil, err
	}
	session.startWriter()
	go session.readLoop()
	return session, nil
}

func (c *HostPlatformClient) interact(ctx context.Context, brickID, commandID string, input any, invocationID, handleID, intent string) (*ConnectorInteraction, error) {
	normalized, err := jsonInput(input)
	if err != nil {
		return nil, err
	}
	value, err := AnyToBrickValue(normalized)
	if err != nil {
		return nil, err
	}
	callCtx := c.withToken(ctx)
	callCtx = metadata.AppendToOutgoingContext(callCtx, TargetBrickIdMD, brickID)
	if invocationID != "" {
		callCtx = metadata.AppendToOutgoingContext(callCtx, InvocationIdMD, invocationID)
	}
	if handleID != "" {
		callCtx = metadata.AppendToOutgoingContext(callCtx, HandleIdMD, handleID)
	}
	callCtx = appendCommandIntent(callCtx, intent)
	callCtx, cancel := context.WithCancel(callCtx)
	stream, err := c.connector.Interact(callCtx)
	if err != nil {
		cancel()
		return nil, err
	}
	session := newConnectorInteraction(stream, cancel)
	if err := session.open(commandID, value); err != nil {
		cancel()
		_ = stream.CloseSend()
		return nil, err
	}
	session.startWriter()
	go session.readLoop()
	return session, nil
}

func newConnectorInteraction(stream interactStream, cancel context.CancelFunc) *ConnectorInteraction {
	session := &ConnectorInteraction{
		stream:     stream,
		cancel:     cancel,
		events:     make(chan any, 256),
		done:       make(chan struct{}),
		pending:    make(map[string]*pendingRequest),
		writerDone: make(chan struct{}),
	}
	session.cond = sync.NewCond(&session.mu)
	return session
}

func (s *ConnectorInteraction) open(commandID string, input *runtimev1.BrickValue) error {
	if err := s.write(&runtimev1.ClientFrame{Body: &runtimev1.ClientFrame_Open{Open: &runtimev1.OpenFrame{CommandId: commandID, Input: input}}}); err != nil {
		return err
	}
	frame, err := s.stream.Recv()
	if err != nil {
		return err
	}
	s.inbound = 1
	if frame.GetOpened() == nil {
		return fmt.Errorf("PROTOCOL_VIOLATION: 服务端首帧必须是 opened")
	}
	return nil
}

func (s *ConnectorInteraction) Send(ctx context.Context, event any) error {
	if err := s.assertWritable(); err != nil {
		return err
	}
	value, err := AnyToBrickValue(event)
	if err != nil {
		return err
	}
	done := make(chan error, 1)
	if err := s.enqueue(outboundItem{
		frame: &runtimev1.ClientFrame{Body: &runtimev1.ClientFrame_Event{Event: &runtimev1.EventFrame{Payload: value}}},
		done:  done,
	}); err != nil {
		return err
	}
	return <-done
}

func (s *ConnectorInteraction) SendLatest(ctx context.Context, key string, event any) error {
	if err := s.assertWritable(); err != nil {
		return err
	}
	value, err := AnyToBrickValue(event)
	if err != nil {
		return err
	}
	return s.enqueue(outboundItem{
		key:   key,
		frame: &runtimev1.ClientFrame{Body: &runtimev1.ClientFrame_Event{Event: &runtimev1.EventFrame{Payload: value}}},
	})
}

// Request 等这一条回复。取消 ctx 只停这一条（写 cancel_request），等同 Node pending.cancel()。
func (s *ConnectorInteraction) Request(ctx context.Context, request any) (any, error) {
	if err := s.assertWritable(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	value, err := AnyToBrickValue(request)
	if err != nil {
		return nil, err
	}
	id := randomMessageID()
	reply := make(chan resultOrError, 1)
	entry := &pendingRequest{state: requestQueued, messageID: id, reply: reply}
	s.mu.Lock()
	if s.inputClosed || s.err != nil || s.closed {
		s.mu.Unlock()
		return nil, fmt.Errorf("interaction 已不能发送")
	}
	s.pending[string(id)] = entry
	s.mu.Unlock()
	sent := make(chan error, 1)
	if err := s.enqueue(outboundItem{
		frame: &runtimev1.ClientFrame{
			Header: &runtimev1.FrameHeader{MessageId: id},
			Body:   &runtimev1.ClientFrame_Request{Request: &runtimev1.RequestFrame{Payload: value}},
		},
		done: sent,
	}); err != nil {
		s.mu.Lock()
		s.finishPendingLocked(entry, err)
		s.mu.Unlock()
		return nil, err
	}
	select {
	case <-ctx.Done():
		s.cancelPending(entry, ctx.Err())
		return nil, ctx.Err()
	case err := <-sent:
		if err != nil {
			s.mu.Lock()
			s.finishPendingLocked(entry, err)
			s.mu.Unlock()
			return nil, err
		}
	case outcome := <-reply:
		return outcome.result, outcome.err
	}
	select {
	case <-ctx.Done():
		s.cancelPending(entry, ctx.Err())
		return nil, ctx.Err()
	case outcome := <-reply:
		return outcome.result, outcome.err
	}
}

func (s *ConnectorInteraction) End(ctx context.Context, timeoutMs ...int) (any, error) {
	if err := s.CloseInput(ctx); err != nil {
		s.Cancel("end-failed")
		return nil, err
	}
	if len(timeoutMs) == 0 {
		return s.Result()
	}
	timeout := time.Duration(timeoutMs[0]) * time.Millisecond
	done := make(chan struct{})
	var result any
	var err error
	go func() {
		result, err = s.Result()
		close(done)
	}()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-done:
		return result, err
	case <-timer.C:
		s.Cancel("DEADLINE_EXCEEDED")
		<-done
		return nil, fmt.Errorf("DEADLINE_EXCEEDED: end 等待超时")
	case <-ctx.Done():
		s.Cancel(ctx.Err().Error())
		<-done
		return nil, ctx.Err()
	}
}

func (s *ConnectorInteraction) CloseInput(ctx context.Context) error {
	s.mu.Lock()
	if s.inputClosed || s.err != nil || s.closed {
		s.mu.Unlock()
		return nil
	}
	s.settleOpenRequestsLocked(!s.peerFinal)
	s.inputClosed = true
	s.writerStop = true
	started := s.writerStarted
	if s.cond != nil {
		s.cond.Broadcast()
	}
	s.mu.Unlock()
	if started {
		<-s.writerDone
	}
	return s.stream.CloseSend()
}

func (s *ConnectorInteraction) Cancel(_ string) {
	s.fail(fmt.Errorf("interaction 已取消"))
	_ = s.stream.CloseSend()
}

func (s *ConnectorInteraction) Events() <-chan any {
	return s.events
}

func (s *ConnectorInteraction) Result() (any, error) {
	<-s.done
	return s.result, s.err
}

func (s *ConnectorInteraction) assertWritable() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.inputClosed || s.err != nil || s.closed {
		return fmt.Errorf("interaction 已不能发送")
	}
	return nil
}

func (s *ConnectorInteraction) write(frame *runtimev1.ClientFrame) error {
	s.mu.Lock()
	s.assignSequenceLocked(frame)
	s.mu.Unlock()
	return s.stream.Send(frame)
}

func (s *ConnectorInteraction) assignSequenceLocked(frame *runtimev1.ClientFrame) {
	s.outbound++
	if frame.Header == nil {
		frame.Header = &runtimev1.FrameHeader{}
	}
	frame.Header.Sequence = s.outbound
}

func (s *ConnectorInteraction) startWriter() {
	s.writerOnce.Do(func() {
		if s.cond == nil {
			s.cond = sync.NewCond(&s.mu)
		}
		if s.writerDone == nil {
			s.writerDone = make(chan struct{})
		}
		s.mu.Lock()
		s.writerStarted = true
		s.mu.Unlock()
		go s.writerLoop()
	})
}

func (s *ConnectorInteraction) enqueue(item outboundItem) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.inputClosed || s.err != nil || s.closed {
		return fmt.Errorf("interaction 已不能发送")
	}
	if item.key != "" {
		for i, existing := range s.outq {
			if existing.key == item.key {
				s.outq[i] = item
				if s.cond != nil {
					s.cond.Signal()
				}
				return nil
			}
		}
	}
	s.outq = append(s.outq, item)
	if s.cond != nil {
		s.cond.Signal()
	}
	return nil
}

func (s *ConnectorInteraction) writerLoop() {
	defer close(s.writerDone)
	for {
		s.mu.Lock()
		for len(s.outq) == 0 && !s.writerStop {
			s.cond.Wait()
		}
		if len(s.outq) == 0 && s.writerStop {
			s.mu.Unlock()
			return
		}
		item := s.outq[0]
		s.outq = s.outq[1:]
		if item.frame != nil && item.frame.GetRequest() != nil {
			key := ""
			if item.frame.GetHeader() != nil {
				key = string(item.frame.GetHeader().GetMessageId())
			}
			entry := s.pending[key]
			if entry == nil || entry.state != requestQueued {
				s.mu.Unlock()
				if item.done != nil {
					item.done <- nil
				}
				continue
			}
			entry.state = requestSent
		}
		s.assignSequenceLocked(item.frame)
		err := s.stream.Send(item.frame)
		s.mu.Unlock()
		if err != nil {
			if item.done != nil {
				item.done <- err
			}
			s.fail(err)
			return
		}
		if item.done != nil {
			item.done <- nil
		}
	}
}

func (s *ConnectorInteraction) readLoop() {
	defer s.finish()
	for {
		frame, err := s.stream.Recv()
		if err != nil {
			if err != io.EOF && s.err == nil {
				s.fail(err)
			}
			return
		}
		incoming := uint64(0)
		if frame.GetHeader() != nil {
			incoming = frame.GetHeader().GetSequence()
		}
		if incoming != s.inbound+1 {
			s.fail(fmt.Errorf("PROTOCOL_VIOLATION: sequence 必须从 1 严格递增，收到 %d", incoming))
			return
		}
		s.inbound = incoming
		if event := frame.GetEvent(); event != nil {
			s.push(brickValueToAny(event.GetPayload()))
			continue
		}
		if response := frame.GetResponse(); response != nil {
			s.onResponse(frame, response)
			continue
		}
		if final := frame.GetFinal(); final != nil {
			s.mu.Lock()
			s.peerFinal = true
			s.settleOpenRequestsLocked(false)
			s.mu.Unlock()
			s.result = brickValueToAny(final.GetResult())
			return
		}
	}
}

func (s *ConnectorInteraction) onResponse(frame *runtimev1.ServerFrame, response *runtimev1.ResponseFrame) {
	key := ""
	if frame.GetHeader() != nil {
		key = string(frame.GetHeader().GetReplyTo())
	}
	s.mu.Lock()
	entry := s.pending[key]
	if entry == nil || entry.state != requestSent {
		s.mu.Unlock()
		return
	}
	if err := response.GetError(); err != nil {
		s.finishPendingLocked(entry, fmt.Errorf("%s", err.GetMessage()))
		s.mu.Unlock()
		return
	}
	s.finishPendingLocked(entry, nil, brickValueToAny(response.GetValue()))
	s.mu.Unlock()
}

func (s *ConnectorInteraction) push(event any) {
	select {
	case s.events <- event:
	case <-s.done:
	}
}

func (s *ConnectorInteraction) fail(err error) {
	s.mu.Lock()
	if s.err != nil {
		s.mu.Unlock()
		return
	}
	s.err = err
	for _, entry := range s.pending {
		if entry.state == requestSent && !s.peerFinal {
			frame := cancelRequestFrame(entry.messageID)
			s.assignSequenceLocked(frame)
			_ = s.stream.Send(frame)
		} else if entry.state == requestQueued {
			s.removeQueuedRequestLocked(string(entry.messageID))
		}
		s.finishPendingLocked(entry, errSessionClosed)
	}
	leftover := s.outq
	s.outq = nil
	s.writerStop = true
	if s.cond != nil {
		s.cond.Broadcast()
	}
	s.mu.Unlock()
	for _, item := range leftover {
		if item.done != nil {
			item.done <- errSessionClosed
		}
	}
	if s.cancel != nil {
		s.cancel()
	}
}

func (s *ConnectorInteraction) finish() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	s.settleOpenRequestsLocked(false)
	close(s.events)
	close(s.done)
	s.mu.Unlock()
}

func (s *ConnectorInteraction) cancelPending(entry *pendingRequest, err error) {
	done := make(chan error, 1)
	s.mu.Lock()
	wait := s.cancelEntryLocked(entry, err, true, done)
	s.mu.Unlock()
	if !wait {
		return
	}
	select {
	case <-done:
	case <-time.After(time.Second):
	}
}

func (s *ConnectorInteraction) settleOpenRequestsLocked(writeCancel bool) {
	for _, entry := range s.pending {
		s.cancelEntryLocked(entry, errSessionClosed, writeCancel, nil)
	}
}

func (s *ConnectorInteraction) cancelEntryLocked(entry *pendingRequest, err error, writeCancel bool, done chan error) bool {
	if entry.state == requestSettled || entry.state == requestCancelled {
		return false
	}
	previous := entry.state
	s.finishPendingLocked(entry, err)
	if previous == requestQueued {
		s.removeQueuedRequestLocked(string(entry.messageID))
		return false
	}
	if previous == requestSent && writeCancel && !s.peerFinal && s.err == nil {
		s.outq = append(s.outq, outboundItem{
			frame: cancelRequestFrame(entry.messageID),
			done:  done,
		})
		if s.cond != nil {
			s.cond.Signal()
		}
		return done != nil
	}
	return false
}

func (s *ConnectorInteraction) finishPendingLocked(entry *pendingRequest, err error, value ...any) {
	if entry.state == requestSettled || entry.state == requestCancelled {
		return
	}
	if err != nil {
		entry.state = requestCancelled
	} else {
		entry.state = requestSettled
	}
	delete(s.pending, string(entry.messageID))
	var result any
	if len(value) > 0 {
		result = value[0]
	}
	select {
	case entry.reply <- resultOrError{result: result, err: err}:
	default:
	}
}

func (s *ConnectorInteraction) removeQueuedRequestLocked(id string) bool {
	for i, item := range s.outq {
		if item.frame == nil || item.frame.GetRequest() == nil {
			continue
		}
		if item.frame.GetHeader() == nil || string(item.frame.GetHeader().GetMessageId()) != id {
			continue
		}
		s.outq = append(s.outq[:i], s.outq[i+1:]...)
		return true
	}
	return false
}

func cancelRequestFrame(messageID []byte) *runtimev1.ClientFrame {
	return &runtimev1.ClientFrame{
		Header: &runtimev1.FrameHeader{MessageId: messageID},
		Body:   &runtimev1.ClientFrame_CancelRequest{CancelRequest: &runtimev1.CancelRequestFrame{}},
	}
}

func randomMessageID() []byte {
	id := make([]byte, 16)
	_, _ = rand.Read(id)
	return id
}

func firstIntent(intent []string) string {
	if len(intent) > 0 {
		return intent[0]
	}
	return ""
}

func appendCommandIntent(ctx context.Context, intent string) context.Context {
	if intent == "call" {
		return metadata.AppendToOutgoingContext(ctx, IntentMD, "call")
	}
	return ctx
}
