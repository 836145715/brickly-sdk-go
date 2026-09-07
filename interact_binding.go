package brickly

import (
	"context"
	"strings"
	"sync"
)

type commandStream interface {
	Send(event any) error
	OnEvent(handler func(event any)) error
	Closed() <-chan struct{}
	HandleRequests(handler func(req any, ctx context.Context) (any, error), concurrency ...int) error
}

type unaryCommandStream struct{}

func (unaryCommandStream) Send(any) error {
	return NewBppError("PROTOCOL_ERROR", "send 需要 interact 调用；本次是 invoke")
}

func (unaryCommandStream) OnEvent(func(any)) error {
	return NewBppError("PROTOCOL_ERROR", "onEvent 需要 interact 调用；本次是 invoke")
}

func (unaryCommandStream) HandleRequests(func(any, context.Context) (any, error), ...int) error {
	return NewBppError("PROTOCOL_ERROR", "handleRequests 需要 interact 调用；本次是 invoke")
}

func (unaryCommandStream) Closed() <-chan struct{} {
	closed := make(chan struct{})
	close(closed)
	return closed
}

type interactCommandStream struct {
	send           func(any) error
	handleRequests func(func(any, context.Context) (any, error), ...int) error
	mu             sync.Mutex
	handler        func(any)
	pending        []any
	closed         chan struct{}
}

func bindInteractStream(
	send func(any) error,
	incoming <-chan any,
	handleRequests func(func(any, context.Context) (any, error), ...int) error,
) *interactCommandStream {
	stream := &interactCommandStream{send: send, handleRequests: handleRequests, closed: make(chan struct{})}
	go func() {
		defer close(stream.closed)
		for event := range incoming {
			stream.mu.Lock()
			handler := stream.handler
			if handler == nil {
				stream.pending = append(stream.pending, event)
				stream.mu.Unlock()
				continue
			}
			stream.mu.Unlock()
			handler(event)
		}
	}()
	return stream
}

func (s *interactCommandStream) Send(event any) error {
	return s.send(event)
}

func (s *interactCommandStream) OnEvent(handler func(any)) error {
	if handler == nil {
		return NewBppError("INVALID_INPUT", "onEvent handler 不能为空")
	}
	s.mu.Lock()
	if s.handler != nil {
		s.mu.Unlock()
		return NewBppError("PROTOCOL_ERROR", "onEvent 只能注册一次")
	}
	s.handler = handler
	pending := s.pending
	s.pending = nil
	s.mu.Unlock()
	for _, event := range pending {
		handler(event)
	}
	return nil
}

func (s *interactCommandStream) Closed() <-chan struct{} {
	return s.closed
}

func (s *interactCommandStream) HandleRequests(handler func(req any, ctx context.Context) (any, error), concurrency ...int) error {
	if s.handleRequests == nil {
		return NewBppError("PROTOCOL_ERROR", "handleRequests 需要 interact 调用；本次是 invoke")
	}
	return mapHandleRequestsError(s.handleRequests(handler, concurrency...))
}

func mapHandleRequestsError(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := err.(*BppError); ok {
		return err
	}
	msg := err.Error()
	switch {
	case strings.HasPrefix(msg, "PROTOCOL_ERROR"):
		return NewBppError("PROTOCOL_ERROR", strings.TrimPrefix(msg, "PROTOCOL_ERROR: "))
	case strings.HasPrefix(msg, "INVALID_INPUT"):
		return NewBppError("INVALID_INPUT", strings.TrimPrefix(msg, "INVALID_INPUT: "))
	default:
		return err
	}
}

// Send 推给调用方。invoke 路径会返回 PROTOCOL_ERROR。
func (c *CommandContext) Send(event any) error {
	return c.stream.Send(event)
}

// OnEvent 收调用方 send。return 值不是回复。invoke 路径会返回 PROTOCOL_ERROR。
func (c *CommandContext) OnEvent(handler func(event any)) error {
	return c.stream.OnEvent(handler)
}

// HandleRequests 注册会话内 request handler；handler 的 return 就是那条 request 的结果。
// 一个 ctx 只能注册一次。invoke 路径会返回 PROTOCOL_ERROR。
func (c *CommandContext) HandleRequests(handler func(req any, ctx context.Context) (any, error), concurrency ...int) error {
	return c.stream.HandleRequests(handler, concurrency...)
}

// Closed 在调用方 closeInput / 断开后关闭。invoke 路径已经是关闭状态。
func (c *CommandContext) Closed() <-chan struct{} {
	return c.stream.Closed()
}
