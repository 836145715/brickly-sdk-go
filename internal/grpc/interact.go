package grpc

import (
	"context"
	"io"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type interactServerSession struct {
	initial any
	ctx     context.Context
	cancel  context.CancelFunc
	events  chan any
	mu      sync.Mutex
	closed  bool
	write   func(*ServerFrame) error
}

func (s *interactServerSession) Initial() any { return s.initial }

func (s *interactServerSession) Context() context.Context { return s.ctx }

func (s *interactServerSession) Events() <-chan any { return s.events }

func (s *interactServerSession) Send(event any) error {
	value, err := AnyToBrickValue(event)
	if err != nil {
		return err
	}
	return s.write(&ServerFrame{Body: &ServerFrame_Event{Event: &EventFrame{Payload: value}}})
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

func echoInteract(stream grpc.BidiStreamingServer[ClientFrame, ServerFrame]) error {
	opened := false
	var inbound uint64
	var sequence uint64
	for {
		frame, err := stream.Recv()
		if err != nil {
			if opened && err == io.EOF {
				sequence++
				return stream.Send(&ServerFrame{
					Header: &FrameHeader{Sequence: sequence},
					Body:   &ServerFrame_Final{Final: &FinalFrame{Result: &BrickValue{Value: &BrickValue_NullValue{NullValue: &NullValue{}}}}},
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
			if err := stream.Send(&ServerFrame{
				Header: &FrameHeader{Sequence: sequence},
				Body:   &ServerFrame_Opened{Opened: &OpenedFrame{}},
			}); err != nil {
				return err
			}
			continue
		}
		if event := frame.GetEvent(); event != nil {
			sequence++
			if err := stream.Send(&ServerFrame{
				Header: &FrameHeader{Sequence: sequence},
				Body:   &ServerFrame_Event{Event: &EventFrame{Payload: event.GetPayload()}},
			}); err != nil {
				return err
			}
		}
	}
}

type interactDuplex interface {
	Recv() (*ClientFrame, error)
	Send(*ServerFrame) error
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
	write := func(next *ServerFrame) error {
		writeMu.Lock()
		defer writeMu.Unlock()
		sequence++
		if next.Header == nil {
			next.Header = &FrameHeader{}
		}
		next.Header.Sequence = sequence
		return stream.Send(next)
	}
	if err := write(&ServerFrame{Body: &ServerFrame_Opened{Opened: &OpenedFrame{}}}); err != nil {
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
			}
		}
	}()
	outcome := <-done
	session.closeInput()
	if outcome.err != nil {
		return StatusFromError(outcome.err)
	}
	result, convErr := AnyToBrickValue(outcome.result)
	if convErr != nil {
		return StatusFromError(convErr)
	}
	return write(&ServerFrame{Body: &ServerFrame_Final{Final: &FinalFrame{Result: result}}})
}

type resultOrError struct {
	result any
	err    error
}
