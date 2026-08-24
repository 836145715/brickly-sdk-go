package brickly

import (
	"sync"
)

type commandStream interface {
	Send(event any) error
	OnEvent(handler func(event any)) error
	Closed() <-chan struct{}
}

type unaryCommandStream struct{}

func (unaryCommandStream) Send(any) error {
	return NewBppError("PROTOCOL_ERROR", "send 需要 interact 调用；本次是 invoke")
}

func (unaryCommandStream) OnEvent(func(any)) error {
	return NewBppError("PROTOCOL_ERROR", "onEvent 需要 interact 调用；本次是 invoke")
}

func (unaryCommandStream) Closed() <-chan struct{} {
	closed := make(chan struct{})
	close(closed)
	return closed
}

type interactCommandStream struct {
	send    func(any) error
	mu      sync.Mutex
	handler func(any)
	pending []any
	closed  chan struct{}
}

func bindInteractStream(send func(any) error, incoming <-chan any) *interactCommandStream {
	stream := &interactCommandStream{send: send, closed: make(chan struct{})}
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

// Send 推给调用方。invoke 路径会返回 PROTOCOL_ERROR。
func (c *CommandContext) Send(event any) error {
	return c.stream.Send(event)
}

// OnEvent 收调用方 send。return 值不是回复。invoke 路径会返回 PROTOCOL_ERROR。
func (c *CommandContext) OnEvent(handler func(event any)) error {
	return c.stream.OnEvent(handler)
}

// Closed 在调用方 closeInput / 断开后关闭。invoke 路径已经是关闭状态。
func (c *CommandContext) Closed() <-chan struct{} {
	return c.stream.Closed()
}
