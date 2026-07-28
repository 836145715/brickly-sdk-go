package brickly

import "context"

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
}

func newCommandContext(
	p *Runtime,
	reqID string,
	commandID string,
	invocation CommandInvocationContext,
	trace *TraceContext,
) *CommandContext {
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

// Context 返回一个在收到 command.cancel 时被取消的 context.Context。
// 业务可直接 select <-ctx.Done() 或传入下游 API（http.Client 等）。
func (c *CommandContext) Context() context.Context { return c.ctx }

// IsCancelled 轮询取消状态（与 Context().Err() 等价但更符合直觉）。
func (c *CommandContext) IsCancelled() bool {
	c.runtime.cancelMu.Lock()
	defer c.runtime.cancelMu.Unlock()
	return c.runtime.cancelled[c.RequestID]
}

// Progress 发送 command.progress。value 通常在 [0,1]；message 可为空。
func (c *CommandContext) Progress(value float64, message string) {
	m := map[string]any{"type": "command.progress", "id": c.RequestID, "progress": value}
	if message != "" {
		m["message"] = message
	}
	c.runtime.transport.send(m)
}

// Output 发送一次性 command.output（相同 name 后者覆盖前者）。
func (c *CommandContext) Output(name string, value any) {
	c.runtime.transport.send(map[string]any{
		"type": "command.output", "id": c.RequestID, "name": name, "value": value,
	})
}

// Chunk 向具名输出追加流式片段（前端按顺序拼接）。
func (c *CommandContext) Chunk(name string, chunk any) {
	c.runtime.transport.send(map[string]any{
		"type": "command.chunk", "id": c.RequestID, "name": name, "chunk": chunk,
	})
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

// Config 返回由 host.hello.config 注入的当前 Profile 配置快照。
func (c *CommandContext) Config() map[string]any { return c.runtime.Config }

// Debug 发送 debug 级别结构化日志到宿主，自动绑定当前 command 的 trace。
func (c *CommandContext) Debug(message string, fields map[string]any) {
	c.runtime.transport.sendLog("debug", message, fields, nil, c.Trace)
}

// Info 发送 info 级别结构化日志到宿主，自动绑定当前 command 的 trace。
func (c *CommandContext) Info(message string, fields map[string]any) {
	c.runtime.transport.sendLog("info", message, fields, nil, c.Trace)
}

// Warn 发送 warn 级别结构化日志到宿主，自动绑定当前 command 的 trace。
func (c *CommandContext) Warn(message string, fields map[string]any) {
	c.runtime.transport.sendLog("warn", message, fields, nil, c.Trace)
}

// Error 发送 error 级别结构化日志到宿主，自动绑定当前 command 的 trace。
func (c *CommandContext) Error(message string, err error, fields map[string]any) {
	var errPayload map[string]any
	if err != nil {
		errPayload = map[string]any{"code": "BRICK_ERROR", "message": err.Error()}
	}
	c.runtime.transport.sendLog("error", message, fields, errPayload, c.Trace)
}

func (c *CommandContext) applyInvokeContext(brickID string, options *invokeOptions) {
	if options.profileID == "" {
		options.profileID = c.Invocation.DependencyProfiles[brickID]
	}
	options.parentRequestID = c.RequestID
	options.trace = c.Trace
}

// Invoke 跨 Brick 调用命令。宿主会自动管理目标 Brick 实例生命周期。
func (c *CommandContext) Invoke(brickID, commandID string, input any, into any, opts ...InvokeOption) error {
	if !c.runtime.isCommandActive(c.RequestID) {
		return parentInvocationRequired("Invoke must run inside an active command handler")
	}
	opts = append(opts, func(options *invokeOptions) {
		c.applyInvokeContext(brickID, options)
	})
	return c.runtime.Invoke(brickID, commandID, input, into, opts...)
}

// InvokeRoot 发起独立 root 调用，并携带当前 command request id 供宿主审计。
func (c *CommandContext) InvokeRoot(brickID, commandID string, input any, into any, opts ...InvokeOption) error {
	options := collectInvokeOptions(opts)
	c.applyInvokeContext(brickID, &options)
	return c.runtime.invokeWithType("host.invokeRoot", brickID, commandID, input, into, options)
}

// InvokeStream 跨 Brick 流式调用命令。
func (c *CommandContext) InvokeStream(brickID, commandID string, input any, opts ...InvokeOption) (<-chan InvokeStreamEvent, <-chan error) {
	if !c.runtime.isCommandActive(c.RequestID) {
		return failedInvokeStream(parentInvocationRequired("InvokeStream must run inside an active command handler"))
	}
	opts = append(opts, func(options *invokeOptions) {
		c.applyInvokeContext(brickID, options)
	})
	return c.runtime.InvokeStream(brickID, commandID, input, opts...)
}

// OpenSession 打开跨 Brick 会话。Close 前，宿主会保持目标 Brick 实例不被回收。
func (c *CommandContext) OpenSession(brickID string, opts ...SessionOption) (*BrickSession, error) {
	if !c.runtime.isCommandActive(c.RequestID) {
		return nil, parentInvocationRequired("OpenSession must run inside an active command handler")
	}
	opts = append(opts, func(options *sessionOptions) {
		if options.profileID == "" {
			options.profileID = c.Invocation.DependencyProfiles[brickID]
		}
		options.parentRequestID = c.RequestID
		options.trace = c.Trace
	})
	return c.runtime.OpenSession(brickID, opts...)
}
