package grpc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"

	runtimev1 "github.com/836145715/brickly-sdk-go/internal/grpc/gen"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	defaultRequestConcurrency = 8
	maxRequestConcurrency     = 128
)

type interactServerSession struct {
	initial     any
	ctx         context.Context
	cancel      context.CancelFunc
	events      chan any
	mu          sync.Mutex
	closed      bool
	stopped     bool
	write       func(*runtimev1.ServerFrame) error
	handler     func(req any, ctx context.Context) (any, error)
	concurrency int
	slots       chan struct{}
	inflight    map[string]context.CancelFunc
	wg          sync.WaitGroup
}

func (s *interactServerSession) Initial() any { return s.initial }

func (s *interactServerSession) Context() context.Context { return s.ctx }

func (s *interactServerSession) Events() <-chan any { return s.events }

func (s *interactServerSession) Send(event any) error {
	value, err := AnyToBrickValue(event)
	if err != nil {
		return err
	}
	return s.write(&runtimev1.ServerFrame{Body: &runtimev1.ServerFrame_Event{Event: &runtimev1.EventFrame{Payload: value}}})
}

func (s *interactServerSession) HandleRequests(handler func(req any, ctx context.Context) (any, error), concurrency ...int) error {
	if handler == nil {
		return fmt.Errorf("INVALID_INPUT: HandleRequests handler 不能为空")
	}
	n := defaultRequestConcurrency
	if len(concurrency) > 0 {
		n = concurrency[0]
	}
	if n < 1 || n > maxRequestConcurrency {
		return fmt.Errorf("INVALID_INPUT: HandleRequests concurrency 必须是 1–128")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.handler != nil {
		return fmt.Errorf("PROTOCOL_ERROR: 一个 session 只能注册一次 request handler")
	}
	s.handler = handler
	s.concurrency = n
	s.slots = make(chan struct{}, n)
	s.inflight = make(map[string]context.CancelFunc)
	return nil
}

func (s *interactServerSession) push(event any) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	ch := s.events
	s.mu.Unlock()
	select {
	case ch <- event:
	case <-s.ctx.Done():
	}
}

func (s *interactServerSession) closeInput() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.closed = true
	close(s.events)
}

func (s *interactServerSession) cancelRequest(messageID []byte) {
	s.mu.Lock()
	cancel := s.inflight[string(messageID)]
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (s *interactServerSession) settleRequests() {
	s.mu.Lock()
	s.stopped = true
	cancels := make([]context.CancelFunc, 0, len(s.inflight))
	for _, cancel := range s.inflight {
		cancels = append(cancels, cancel)
	}
	s.mu.Unlock()
	for _, cancel := range cancels {
		cancel()
	}
	s.wg.Wait()
}

func (s *interactServerSession) onRequest(payload *runtimev1.BrickValue, replyTo []byte) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.dispatchRequest(payload, replyTo)
	}()
}

func (s *interactServerSession) dispatchRequest(payload *runtimev1.BrickValue, replyTo []byte) {
	s.mu.Lock()
	handler := s.handler
	stopped := s.stopped
	slots := s.slots
	s.mu.Unlock()
	if handler == nil {
		_ = s.writeResponse(replyTo, nil, requestBrickError("REQUEST_HANDLER_UNAVAILABLE", "未注册 request handler"))
		return
	}
	if stopped {
		_ = s.writeResponse(replyTo, nil, requestBrickError("CANCELLED", "request 已取消"))
		return
	}
	reqCtx, cancel := context.WithCancel(s.ctx)
	key := string(replyTo)
	s.mu.Lock()
	if s.inflight == nil {
		s.inflight = make(map[string]context.CancelFunc)
	}
	s.inflight[key] = cancel
	s.mu.Unlock()
	defer func() {
		cancel()
		s.mu.Lock()
		delete(s.inflight, key)
		s.mu.Unlock()
	}()
	if slots == nil {
		slots = make(chan struct{}, defaultRequestConcurrency)
	}
	select {
	case slots <- struct{}{}:
	case <-reqCtx.Done():
		_ = s.writeResponse(replyTo, nil, requestBrickError("CANCELLED", "request 已取消"))
		return
	}
	defer func() { <-slots }()
	if reqCtx.Err() != nil {
		_ = s.writeResponse(replyTo, nil, requestBrickError("CANCELLED", "request 已取消"))
		return
	}
	result, err := handler(brickValueToAny(payload), reqCtx)
	if err != nil {
		_ = s.writeResponse(replyTo, nil, encodeRequestError(err, reqCtx.Err() != nil))
		return
	}
	value, convErr := AnyToBrickValue(result)
	if convErr != nil {
		_ = s.writeResponse(replyTo, nil, encodeRequestError(convErr, false))
		return
	}
	_ = s.writeResponse(replyTo, value, nil)
}

func (s *interactServerSession) writeResponse(replyTo []byte, value *runtimev1.BrickValue, brickErr *runtimev1.BrickError) error {
	frame := &runtimev1.ServerFrame{
		Header: &runtimev1.FrameHeader{ReplyTo: append([]byte(nil), replyTo...)},
	}
	if brickErr != nil {
		frame.Body = &runtimev1.ServerFrame_Response{Response: &runtimev1.ResponseFrame{
			Outcome: &runtimev1.ResponseFrame_Error{Error: brickErr},
		}}
	} else {
		frame.Body = &runtimev1.ServerFrame_Response{Response: &runtimev1.ResponseFrame{
			Outcome: &runtimev1.ResponseFrame_Value{Value: value},
		}}
	}
	return s.write(frame)
}

func requestBrickError(code, message string) *runtimev1.BrickError {
	if strings.Contains(message, "token") {
		message = strings.ReplaceAll(message, "token", "***")
	}
	return &runtimev1.BrickError{Code: code, Message: message, Retryable: false}
}

func encodeRequestError(err error, cancelled bool) *runtimev1.BrickError {
	if cancelled || errors.Is(err, context.Canceled) {
		return requestBrickError("CANCELLED", "request 已取消")
	}
	code := "INTERNAL"
	if coded, ok := err.(interface{ BrickCode() string }); ok {
		code = normalizeRequestErrorCode(coded.BrickCode())
	}
	return requestBrickError(code, err.Error())
}

func normalizeRequestErrorCode(code string) string {
	switch code {
	case "INTERNAL_ERROR":
		return "INTERNAL"
	case "PROTOCOL_ERROR":
		return "PROTOCOL_VIOLATION"
	case "":
		return "INTERNAL"
	default:
		return code
	}
}

func echoInteract(stream grpc.BidiStreamingServer[runtimev1.ClientFrame, runtimev1.ServerFrame]) error {
	opened := false
	var inbound uint64
	var sequence uint64
	for {
		frame, err := stream.Recv()
		if err != nil {
			if opened && err == io.EOF {
				sequence++
				return stream.Send(&runtimev1.ServerFrame{
					Header: &runtimev1.FrameHeader{Sequence: sequence},
					Body:   &runtimev1.ServerFrame_Final{Final: &runtimev1.FinalFrame{Result: &runtimev1.BrickValue{Value: &runtimev1.BrickValue_NullValue{NullValue: &runtimev1.NullValue{}}}}},
				})
			}
			return err
		}
		incoming := uint64(0)
		if frame.GetHeader() != nil {
			incoming = frame.GetHeader().GetSequence()
		}
		if incoming != inbound+1 {
			return status.Error(codes.Internal, "PROTOCOL_VIOLATION: sequence")
		}
		inbound = incoming
		if !opened {
			if frame.GetOpen() == nil || incoming != 1 {
				return status.Error(codes.Internal, "PROTOCOL_VIOLATION: 首帧必须是 open")
			}
			opened = true
			sequence++
			if err := stream.Send(&runtimev1.ServerFrame{
				Header: &runtimev1.FrameHeader{Sequence: sequence},
				Body:   &runtimev1.ServerFrame_Opened{Opened: &runtimev1.OpenedFrame{}},
			}); err != nil {
				return err
			}
			continue
		}
		if event := frame.GetEvent(); event != nil {
			sequence++
			if err := stream.Send(&runtimev1.ServerFrame{
				Header: &runtimev1.FrameHeader{Sequence: sequence},
				Body:   &runtimev1.ServerFrame_Event{Event: &runtimev1.EventFrame{Payload: event.GetPayload()}},
			}); err != nil {
				return err
			}
		}
	}
}

type interactDuplex interface {
	Recv() (*runtimev1.ClientFrame, error)
	Send(*runtimev1.ServerFrame) error
	Context() context.Context
}

func (s *commandServer) dispatchInteract(stream interactDuplex) error {
	frame, err := stream.Recv()
	if err != nil {
		return err
	}
	if frame.GetOpen() == nil || (frame.GetHeader() != nil && frame.GetHeader().GetSequence() != 1) {
		return status.Error(codes.Internal, "PROTOCOL_VIOLATION: 首帧必须是 open")
	}
	var sequence uint64
	var inbound uint64 = 1
	writeMu := sync.Mutex{}
	write := func(next *runtimev1.ServerFrame) error {
		writeMu.Lock()
		defer writeMu.Unlock()
		sequence++
		if next.Header == nil {
			next.Header = &runtimev1.FrameHeader{}
		}
		next.Header.Sequence = sequence
		return stream.Send(next)
	}
	if err := write(&runtimev1.ServerFrame{Body: &runtimev1.ServerFrame_Opened{Opened: &runtimev1.OpenedFrame{}}}); err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()
	session := &interactServerSession{
		initial: brickValueToAny(frame.GetOpen().GetInput()),
		ctx:     ctx,
		cancel:  cancel,
		events:  make(chan any, 16),
		write:   write,
	}
	done := make(chan resultOrError, 1)
	commandID := frame.GetOpen().GetCommandId()
	go func() {
		result, interactErr := s.interact(commandID, session)
		done <- resultOrError{result: result, err: interactErr}
	}()
	go func() {
		for {
			next, recvErr := stream.Recv()
			if recvErr != nil {
				session.closeInput()
				return
			}
			incoming := uint64(0)
			if next.GetHeader() != nil {
				incoming = next.GetHeader().GetSequence()
			}
			if incoming != inbound+1 {
				session.closeInput()
				return
			}
			inbound = incoming
			if event := next.GetEvent(); event != nil {
				session.push(brickValueToAny(event.GetPayload()))
				continue
			}
			if req := next.GetRequest(); req != nil {
				messageID := []byte(nil)
				if next.GetHeader() != nil {
					messageID = next.GetHeader().GetMessageId()
				}
				if len(messageID) != 16 {
					session.closeInput()
					return
				}
				session.onRequest(req.GetPayload(), messageID)
				continue
			}
			if next.GetCancelRequest() != nil {
				messageID := []byte(nil)
				if next.GetHeader() != nil {
					messageID = next.GetHeader().GetMessageId()
				}
				if len(messageID) == 16 {
					session.cancelRequest(messageID)
				}
				continue
			}
		}
	}()
	outcome := <-done
	session.closeInput()
	session.settleRequests()
	if outcome.err != nil {
		return StatusFromError(outcome.err)
	}
	result, convErr := AnyToBrickValue(outcome.result)
	if convErr != nil {
		return StatusFromError(convErr)
	}
	return write(&runtimev1.ServerFrame{Body: &runtimev1.ServerFrame_Final{Final: &runtimev1.FinalFrame{Result: result}}})
}

type resultOrError struct {
	result any
	err    error
}
