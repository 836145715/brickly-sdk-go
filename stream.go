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
	Interact(ctx context.Context, command string, input any, opts ...InteractOptions) (Interaction, error)
}

// InteractOptions 是 interact 的参数。OnEvent 必须传入；返回值不是回复。
type InteractOptions struct {
	OnEvent func(event any)
}

// CallOptions 是 Call 的可选参数。OnEvent 必须传入；返回值不是回复。
type CallOptions struct {
	OnEvent   func(event any)
	TimeoutMs *int
}

func requireInteractOnEvent(opts []InteractOptions) (InteractOptions, error) {
	var options InteractOptions
	if len(opts) > 0 {
		options = opts[0]
	}
	if options.OnEvent == nil {
		return options, fmt.Errorf("INVALID_ARGUMENT: interact 必须传入 OnEvent")
	}
	return options, nil
}

func pumpSessionEvents(session Interaction, onEvent func(event any)) {
	transport, ok := session.(interactionTransport)
	if !ok || onEvent == nil {
		return
	}
	go func() {
		for event := range transport.Events() {
			onEvent(event)
		}
	}()
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
	session, err := client.Interact(ctx, command, input, InteractOptions{OnEvent: options.OnEvent})
	if err != nil {
		return nil, err
	}
	if options.TimeoutMs != nil {
		return session.End(ctx, *options.TimeoutMs)
	}
	return session.End(ctx)
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
