package brickly

import "context"

// ToolHandleState 是公开 Handle 状态。
type ToolHandleState string

const (
	ToolHandleActive   ToolHandleState = "active"
	ToolHandleDisposed ToolHandleState = "disposed"
	ToolHandleStopping ToolHandleState = "stopping"
	ToolHandleStopped  ToolHandleState = "stopped"
	ToolHandleFailed   ToolHandleState = "failed"
)

// StartToolOptions 控制显式 start。
type StartToolOptions struct {
	ProfileID string
	Owner     context.Context
}

// ToolLifetimeHost 是宿主 Lifetime 端口；测试可注入。
type ToolLifetimeHost interface {
	Start(ctx context.Context, tool BrickRef, options StartToolOptions) (handleID string, err error)
	Invoke(ctx context.Context, handleID, commandID string, input any) (any, error)
	Interact(ctx context.Context, handleID, commandID string, input any) (any, error)
	Dispose(ctx context.Context, handleID string) error
	Stop(ctx context.Context, handleID string) error
	InvokeOnce(ctx context.Context, tool BrickRef, commandID string, input any) (any, error)
	InteractOnce(ctx context.Context, tool BrickRef, commandID string, input any) (any, error)
}

// ToolHandle 是调用方持有的长期所有权。不暴露 lifetimeId / retainer。
type ToolHandle struct {
	State ToolHandleState
	id    string
	host  ToolLifetimeHost
}

// Invoke 在 Handle 仍然 active 时发起一次性命令。
func (h *ToolHandle) Invoke(ctx context.Context, commandID string, input any) (any, error) {
	if err := h.assertCallable(); err != nil {
		return nil, err
	}
	return h.host.Invoke(ctx, h.id, commandID, input)
}

// Interact 在 Handle 仍然 active 时发起交互会话。
func (h *ToolHandle) Interact(ctx context.Context, commandID string, input any) (any, error) {
	if err := h.assertCallable(); err != nil {
		return nil, err
	}
	return h.host.Interact(ctx, h.id, commandID, input)
}

// Dispose 释放 Handle 所有权，不取消在途 Call。
func (h *ToolHandle) Dispose(ctx context.Context) error {
	if h.State == ToolHandleDisposed {
		return nil
	}
	if err := h.host.Dispose(ctx, h.id); err != nil {
		return err
	}
	if h.State != ToolHandleFailed {
		h.State = ToolHandleDisposed
	}
	return nil
}

// Close 是 Dispose 的别名，无 force 参数。
func (h *ToolHandle) Close(ctx context.Context) error {
	return h.Dispose(ctx)
}

// Stop 取消该 Lifetime 的调用并关闭全部窗口。
func (h *ToolHandle) Stop(ctx context.Context) error {
	if h.State == ToolHandleStopped || h.State == ToolHandleFailed {
		return nil
	}
	h.State = ToolHandleStopping
	if err := h.host.Stop(ctx, h.id); err != nil {
		return err
	}
	if h.State != ToolHandleFailed && h.State != ToolHandleDisposed {
		h.State = ToolHandleStopped
	}
	return nil
}

func (h *ToolHandle) assertCallable() error {
	if h.State == ToolHandleFailed {
		return NewBppError("RUNTIME_UNAVAILABLE", "Runtime 已失败，Handle 不可用")
	}
	if h.State != ToolHandleActive {
		return NewBppError("HANDLE_CLOSED", "ToolHandle 已关闭，不能发起新调用")
	}
	return nil
}

// ToolSdk 只暴露 start / invoke / interact。
type ToolSdk struct {
	host ToolLifetimeHost
}

// NewToolSdk 构造公开工具 SDK。
func NewToolSdk(host ToolLifetimeHost) *ToolSdk {
	return &ToolSdk{host: host}
}

// Start 成功表示 Runtime Ready。
func (s *ToolSdk) Start(ctx context.Context, tool BrickRef, options StartToolOptions) (*ToolHandle, error) {
	id, err := s.host.Start(ctx, tool, options)
	if err != nil {
		return nil, err
	}
	handle := &ToolHandle{State: ToolHandleActive, id: id, host: s.host}
	if options.Owner != nil {
		go func() {
			<-options.Owner.Done()
			_ = handle.Dispose(context.Background())
		}()
	}
	return handle, nil
}

// Invoke 是无 Handle 的一次性调用。
func (s *ToolSdk) Invoke(ctx context.Context, tool BrickRef, commandID string, input any) (any, error) {
	return s.host.InvokeOnce(ctx, tool, commandID, input)
}

// Interact 是无 Handle 的一次性交互。
func (s *ToolSdk) Interact(ctx context.Context, tool BrickRef, commandID string, input any) (any, error) {
	return s.host.InteractOnce(ctx, tool, commandID, input)
}
