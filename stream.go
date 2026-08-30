package brickly

import (
	"context"
	"fmt"
	"time"
)

// Interaction 是 interact 返回的领域会话。调用方不接触 gRPC 类型。
type Interaction interface {
	Send(ctx context.Context, event any) error
	SendLatest(ctx context.Context, key string, event any) error
	Request(ctx context.Context, request any) (any, error)
	End(ctx context.Context, timeoutMs ...int) (any, error)
	Cancel(reason string)
}

// interactionTransport 是实现层半关闭与收事件的内部面，不出现在公开文档。
type interactionTransport interface {
	Interaction
	CloseInput(ctx context.Context) error
	Events() <-chan any
	Result() (any, error)
}

// BrickClient 只暴露 invoke / interact 两种协议形态。Call 是糖。
type BrickClient interface {
	Invoke(ctx context.Context, command string, input any) (any, error)
	Interact(ctx context.Context, command string, input any) (Interaction, error)
}

// CallOptions 是 Call 的可选参数。OnEvent 必须传入；返回值不是回复。
type CallOptions struct {
	OnEvent   func(event any)
	TimeoutMs *int
}

// Call 是单次开场糖：Interact + 立刻 End。必须与命令 mode=call 对齐。
func Call(ctx context.Context, client BrickClient, command string, input any, opts ...CallOptions) (any, error) {
	var options CallOptions
	if len(opts) > 0 {
		options = opts[0]
	}
	if options.OnEvent == nil {
		return nil, fmt.Errorf("INVALID_ARGUMENT: call 必须传入 OnEvent")
	}
	session, err := client.Interact(ctx, command, input)
	if err != nil {
		return nil, err
	}
	transport, ok := session.(interactionTransport)
	if !ok {
		if options.TimeoutMs != nil {
			return session.End(ctx, *options.TimeoutMs)
		}
		return session.End(ctx)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for event := range transport.Events() {
			options.OnEvent(event)
		}
	}()
	if options.TimeoutMs != nil {
		result, endErr := session.End(ctx, *options.TimeoutMs)
		<-done
		return result, endErr
	}
	result, endErr := session.End(ctx)
	<-done
	return result, endErr
}

func endInteraction(ctx context.Context, session interactionTransport, timeoutMs ...int) (any, error) {
	if err := session.CloseInput(ctx); err != nil {
		session.Cancel("call-end-failed")
		return nil, err
	}
	if len(timeoutMs) == 0 {
		return session.Result()
	}
	timeout := time.Duration(timeoutMs[0]) * time.Millisecond
	done := make(chan struct{})
	var result any
	var err error
	go func() {
		result, err = session.Result()
		close(done)
	}()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-done:
		return result, err
	case <-timer.C:
		session.Cancel("DEADLINE_EXCEEDED")
		<-done
		return nil, fmt.Errorf("DEADLINE_EXCEEDED: end 等待超时")
	case <-ctx.Done():
		session.Cancel(ctx.Err().Error())
		<-done
		return nil, ctx.Err()
	}
}
