package grpc

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"sync"
	"time"

	"google.golang.org/grpc/metadata"
)

type interactStream interface {
	Send(*ClientFrame) error
	Recv() (*ServerFrame, error)
	CloseSend() error
}

// ConnectorInteraction 是 Runtime → Host Connector.Interact 的客户端会话。
type ConnectorInteraction struct {
	stream   interactStream
	cancel   context.CancelFunc
	mu       sync.Mutex
	outbound uint64
	inbound  uint64
	events   chan any
	done     chan struct{}
	result   any
	err      error
	pending     map[string]chan resultOrError
	closed      bool
	inputClosed bool
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
	session := &ConnectorInteraction{
		stream:  stream,
		cancel:  cancel,
		events:  make(chan any, 256),
		done:    make(chan struct{}),
		pending: make(map[string]chan resultOrError),
	}
	if err := session.open(commandID, value); err != nil {
		cancel()
		_ = stream.CloseSend()
		return nil, err
	}
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
	session := &ConnectorInteraction{
		stream:  stream,
		cancel:  cancel,
		events:  make(chan any, 256),
		done:    make(chan struct{}),
		pending: make(map[string]chan resultOrError),
	}
	if err := session.open(commandID, value); err != nil {
		cancel()
		_ = stream.CloseSend()
		return nil, err
	}
	go session.readLoop()
	return session, nil
}

func (s *ConnectorInteraction) open(commandID string, input *BrickValue) error {
	if err := s.write(&ClientFrame{Body: &ClientFrame_Open{Open: &OpenFrame{CommandId: commandID, Input: input}}}); err != nil {
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
	return s.write(&ClientFrame{Body: &ClientFrame_Event{Event: &EventFrame{Payload: value}}})
}

func (s *ConnectorInteraction) SendLatest(ctx context.Context, _ string, event any) error {
	return s.Send(ctx, event)
}

func (s *ConnectorInteraction) Request(ctx context.Context, request any) (any, error) {
	if err := s.assertWritable(); err != nil {
		return nil, err
	}
	value, err := AnyToBrickValue(request)
	if err != nil {
		return nil, err
	}
	id := randomMessageID()
	reply := make(chan resultOrError, 1)
	s.mu.Lock()
	s.pending[string(id)] = reply
	s.mu.Unlock()
	if err := s.write(&ClientFrame{
		Header: &FrameHeader{MessageId: id},
		Body:   &ClientFrame_Request{Request: &RequestFrame{Payload: value}},
	}); err != nil {
		s.mu.Lock()
		delete(s.pending, string(id))
		s.mu.Unlock()
		return nil, err
	}
	select {
	case <-ctx.Done():
		s.mu.Lock()
		delete(s.pending, string(id))
		s.mu.Unlock()
		_ = s.write(&ClientFrame{
			Header: &FrameHeader{MessageId: id},
			Body:   &ClientFrame_CancelRequest{CancelRequest: &CancelRequestFrame{}},
		})
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
	s.inputClosed = true
	s.mu.Unlock()
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

func (s *ConnectorInteraction) write(frame *ClientFrame) error {
	s.mu.Lock()
	s.outbound++
	if frame.Header == nil {
		frame.Header = &FrameHeader{}
	}
	frame.Header.Sequence = s.outbound
	s.mu.Unlock()
	return s.stream.Send(frame)
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
			s.result = brickValueToAny(final.GetResult())
			return
		}
	}
}

func (s *ConnectorInteraction) onResponse(frame *ServerFrame, response *ResponseFrame) {
	key := ""
	if frame.GetHeader() != nil {
		key = string(frame.GetHeader().GetReplyTo())
	}
	s.mu.Lock()
	reply := s.pending[key]
	delete(s.pending, key)
	s.mu.Unlock()
	if reply == nil {
		return
	}
	if err := response.GetError(); err != nil {
		reply <- resultOrError{err: fmt.Errorf("%s", err.GetMessage())}
		return
	}
	reply <- resultOrError{result: brickValueToAny(response.GetValue())}
}

func (s *ConnectorInteraction) push(event any) {
	select {
	case s.events <- event:
	case <-s.done:
	}
}

func (s *ConnectorInteraction) fail(err error) {
	s.mu.Lock()
	already := s.err != nil
	if !already {
		s.err = err
	}
	s.mu.Unlock()
	if already {
		return
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
	close(s.events)
	close(s.done)
	for _, reply := range s.pending {
		reply <- resultOrError{err: s.err}
	}
	s.pending = nil
	s.mu.Unlock()
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
