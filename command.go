package brickly

import (
	"context"
	"io"
)

// CommandContext 是 CommandHandler 的第一个参数。等价于 Node SDK 的
// CommandContext，但用 Go 惯用法：Context() 返回 context.Context 用于取消；
// IsCancelled() 提供协作式取消轮询。
type CommandContext struct {
	RequestID  string
	CommandID  string
	Invocation CommandInvocationContext
	Trace      *TraceContext

	runtime *Runtime
	ctx     context.Context
	cancel  context.CancelFunc

	platform *PlatformAPI
	system   *SystemAPI
	stream   commandStream
}

func newCommandContext(
	p *Runtime,
	reqID string,
	commandID string,
	invocation CommandInvocationContext,
	trace *TraceContext,
) *CommandContext {
	if invocation.Source == "" {
		invocation.Source = "unknown"
	}
	ctx, cancel := context.WithCancel(context.Background())
	platform := newPlatformAPI(p, trace)
	c := &CommandContext{
		RequestID:  reqID,
		CommandID:  commandID,
		Invocation: invocation,
		Trace:      trace,
		runtime:    p,
		ctx:        ctx,
		cancel:     cancel,
		platform:   platform,
		system:     platform.System,
		stream:     unaryCommandStream{},
	}
	p.cancelMu.Lock()
	p.cancelHandlers[reqID] = cancel
	already := p.cancelled[reqID]
	p.cancelMu.Unlock()
	if already {
		cancel()
	}
	return c
}

func (p *Runtime) isCommandActive(reqID string) bool {
	p.cancelMu.Lock()
	defer p.cancelMu.Unlock()
	return p.cancelHandlers[reqID] != nil
}

// Context 返回一个在命令取消时被取消的 context.Context。
// 业务可直接 select <-ctx.Done() 或传入下游 API（http.Client 等）。
func (c *CommandContext) Context() context.Context { return c.ctx }

// IsCancelled 轮询取消状态（与 Context().Err() 等价但更符合直觉）。
func (c *CommandContext) IsCancelled() bool {
	c.runtime.cancelMu.Lock()
	defer c.runtime.cancelMu.Unlock()
	return c.runtime.cancelled[c.RequestID]
}

// CreateResource 创建绑定当前 command 生命周期的资源。
func (c *CommandContext) CreateResource(content any, options *ResourceCreateOptions) (*ResourceHandle, error) {
	return c.runtime.createResource(content, options, c.RequestID)
}

// CreateResourceFrom 从 reader 创建绑定当前 command 生命周期的资源。
func (c *CommandContext) CreateResourceFrom(reader io.Reader, options *ResourceCreateOptions) (*ResourceHandle, error) {
	return c.runtime.createResourceFrom(reader, options, c.RequestID)
}

func (c *CommandContext) CreateResourceWriter(options *ResourceCreateOptions) (*ResourceWriter, error) {
	return c.runtime.createResourceWriter(options, c.RequestID)
}

// UI 返回当前 command 作用域下的 UI 门面。
func (c *CommandContext) UI() *ScopedUI {
	return &ScopedUI{runtime: c.runtime, parentRequestID: c.RequestID, trace: c.Trace}
}

// Events 返回 Brick 级事件总线（与 Runtime.Events 同源）。
func (c *CommandContext) Events() *EventBus { return c.runtime.Events }

// Platform 返回宿主平台能力门面（与 Runtime.Platform 同源），携带当前 command 的 trace。
func (c *CommandContext) Platform() *PlatformAPI {
	return c.platform
}

// System 返回宿主系统能力门面（与 Runtime.System / Runtime.Platform.System 同源），携带当前 command 的 trace。
func (c *CommandContext) System() *SystemAPI {
	return c.system
}

// Config 返回由 Host 注入的当前 Profile 配置快照。
func (c *CommandContext) Config() map[string]any { return c.runtime.Config }

// Debug 挂到当前 command；handler 返回后的异步日志请继续用 Runtime.Debug。
func (c *CommandContext) Debug(message string, fields map[string]any) {
	c.runtime.emitBrickLog("debug", message, nil, fields, c.RequestID)
}

// Info 挂到当前 command；handler 返回后的异步日志请继续用 Runtime.Info。
func (c *CommandContext) Info(message string, fields map[string]any) {
	c.runtime.emitBrickLog("info", message, nil, fields, c.RequestID)
}

// Warn 挂到当前 command；handler 返回后的异步日志请继续用 Runtime.Warn。
func (c *CommandContext) Warn(message string, fields map[string]any) {
	c.runtime.emitBrickLog("warn", message, nil, fields, c.RequestID)
}

// Error 挂到当前 command；handler 返回后的异步日志请继续用 Runtime.Error。
func (c *CommandContext) Error(message string, err error, fields map[string]any) {
	c.runtime.emitBrickLog("error", message, err, fields, c.RequestID)
}

// Dependencies 返回绑定当前 command parent、trace 与 Profile 的依赖注册表。
func (c *CommandContext) Dependencies() *ScopedDependencyRegistry {
	return &ScopedDependencyRegistry{
		registry:           c.runtime.Dependencies,
		parentRequestID:    c.RequestID,
		trace:              c.Trace,
		dependencyProfiles: c.Invocation.DependencyProfiles,
	}
}
