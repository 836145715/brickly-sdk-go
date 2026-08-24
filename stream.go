package brickly

import "context"

// Interaction 是 interact 返回的领域会话。调用方不接触 gRPC 类型。
type Interaction interface {
	Send(ctx context.Context, event any) error
	SendLatest(ctx context.Context, key string, event any) error
	Request(ctx context.Context, request any) (any, error)
	CloseInput(ctx context.Context) error
	Cancel(reason string)
	Events() <-chan any
	Result() (any, error)
}

// BrickClient 只暴露 invoke / interact 两种协议形态。Call / Stream 是糖。
type BrickClient interface {
	Invoke(ctx context.Context, command string, input any) (any, error)
	Interact(ctx context.Context, command string, input any) (Interaction, error)
}

// CallOptions 是 Call 的可选参数。OnEvent 的返回值不是回复。
type CallOptions struct {
	OnEvent func(event any)
}

// Call 是单次阻塞糖：没有 OnEvent 走 Invoke；有则 Interact + CloseInput + 收事件 + Result。
func Call(ctx context.Context, client BrickClient, command string, input any, opts ...CallOptions) (any, error) {
	var onEvent func(any)
	if len(opts) > 0 {
		onEvent = opts[0].OnEvent
	}
	if onEvent == nil {
		return client.Invoke(ctx, command, input)
	}
	session, err := client.Interact(ctx, command, input)
	if err != nil {
		return nil, err
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for event := range session.Events() {
			onEvent(event)
		}
	}()
	if err := session.CloseInput(ctx); err != nil {
		session.Cancel("call-close-failed")
		<-done
		return nil, err
	}
	result, err := session.Result()
	<-done
	return result, err
}

// Stream 是 interact + CloseInput 的便利封装，不是第三种协议。
func Stream(ctx context.Context, client BrickClient, command string, input any) (<-chan any, error) {
	session, err := client.Interact(ctx, command, input)
	if err != nil {
		return nil, err
	}
	if err := session.CloseInput(ctx); err != nil {
		return nil, err
	}
	return session.Events(), nil
}
