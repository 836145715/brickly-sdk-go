// Package brickly 是 Brickly Brick Go runtime 官方 SDK。
//
// 与 @syllm/brickly-sdk (Node) 保持一致的 API 表面与语义：
//
//	p := brickly.New()
//	p.OnCommand("hello", func(ctx *brickly.CommandContext, input json.RawMessage) (any, error) {
//	    return map[string]any{"ok": true}, nil
//	})
//	p.Start() // 阻塞，直到 gRPC Runtime 退出
//
// 生产路径只走 Host gRPC；业务日志必须用 Info/Warn/Error/Debug。
package brickly

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"

	"context"

	"google.golang.org/grpc/metadata"

	runtimegrpc "github.com/836145715/brickly-sdk-go/internal/grpc"
)

// CommandHandler 是 OnCommand 注册的业务处理函数。
//
// 返回 (result, nil) 时 SDK 通过 gRPC 回传结果；
// 返回 (_, err) 时 SDK 通过 gRPC 回传错误（*BppError 会保留其 code）。
type CommandHandler func(ctx *CommandContext, input json.RawMessage) (any, error)

// ReadyHandler 在 gRPC Runtime 注册成功后异步触发。
type ReadyHandler func() error

// ShutdownHandler 在 Runtime 关闭时触发。
type ShutdownHandler func() error

const maxTerminalWindowEventIDs = 1024

// Runtime 是 SDK 主入口，通过 New 创建。
type Runtime struct {
	// UI / Events / Platform 与 Node SDK 的 brick.ui / brick.events /
	// brick.platform 同源。System 是 Platform.System 的便捷别名。
	UI           *UI
	Events       *EventBus
	Platform     *PlatformAPI
	System       *SystemAPI
	Dependencies *DependencyRegistry
	Storage      *StorageAPI
	Config       map[string]any

	mu              sync.RWMutex
	commandHandlers map[string]CommandHandler
	readyHandler    ReadyHandler
	shutdownHandler ShutdownHandler

	cancelMu       sync.Mutex
	cancelHandlers map[string]context.CancelFunc
	cancelled      map[string]bool

	windowsMu sync.RWMutex
	windows   map[int64]*WindowHandle

	terminalWindowEventsMu sync.Mutex
	terminalWindowEventIDs map[string]struct{}
	terminalWindowOrder    []string

	started           atomic.Bool
	done              chan struct{}
	doneOnce          sync.Once
	grpcHandle        *runtimegrpc.RuntimeHandle
	grpcResources     *runtimegrpc.HostResourceClient
	grpcPlatform      *runtimegrpc.HostPlatformClient
	grpcStorage       *runtimegrpc.HostBrickStorageClient
	currentRequestID  atomic.Value
	currentCommandCtx atomic.Value
	inCommand         atomic.Bool

	eventSubsMu sync.Mutex
	eventSubs   map[string]func()
}

// New 创建并返回一个 Runtime 实例。不会连接 Host——那是 Start 的职责。
func New() *Runtime {
	p := &Runtime{
		commandHandlers:        make(map[string]CommandHandler),
		cancelHandlers:         make(map[string]context.CancelFunc),
		cancelled:              make(map[string]bool),
		windows:                make(map[int64]*WindowHandle),
		terminalWindowEventIDs: make(map[string]struct{}),
		done:                   make(chan struct{}),
	}
	p.UI = &UI{runtime: p}
	p.Events = &EventBus{runtime: p}
	p.Platform = newPlatformAPI(p, nil)
	p.System = p.Platform.System
	p.Dependencies = newDependencyRegistry(p)
	p.Storage = newStorageAPI(p)
	p.Config = map[string]any{}
	return p
}

// OnCommand 注册 command 处理器。返回 *Runtime 以便链式调用。
func (p *Runtime) OnCommand(commandID string, fn CommandHandler) *Runtime {
	p.mu.Lock()
	p.commandHandlers[commandID] = fn
	p.mu.Unlock()
	return p
}

// OnReady 注册 ready 钩子（gRPC Runtime 注册成功后异步触发）。
func (p *Runtime) OnReady(fn ReadyHandler) *Runtime {
	p.mu.Lock()
	p.readyHandler = fn
	p.mu.Unlock()
	return p
}

// OnShutdown 注册 shutdown 钩子。
func (p *Runtime) OnShutdown(fn ShutdownHandler) *Runtime {
	p.mu.Lock()
	p.shutdownHandler = fn
	p.mu.Unlock()
	return p
}

// Start 启动 Runtime：必须由 Host 注入 gRPC 环境。
// 典型用法：main 函数最后一行调用。
func (p *Runtime) Start() {
	if !p.started.CompareAndSwap(false, true) {
		return
	}
	if os.Getenv(runtimegrpc.HostEndpointEnv) == "" {
		p.Error("BRICKLY_HOST_ENDPOINT is required; Runtime 只走 Host gRPC", nil, nil)
		p.started.Store(false)
		p.signalDone()
		return
	}
	p.Config = ReadInjectedProfileConfig()
	if err := p.startGRPC(); err != nil {
		p.Error("grpc runtime start failed", err, nil)
		p.signalDone()
	}
	<-p.done
	if p.grpcHandle != nil {
		p.grpcHandle.Close()
	}
	if p.grpcResources != nil {
		_ = p.grpcResources.Close()
		p.grpcResources = nil
	}
	p.clearEventSubs()
	if p.grpcPlatform != nil {
		_ = p.grpcPlatform.Close()
		p.grpcPlatform = nil
	}
	if p.grpcStorage != nil {
		_ = p.grpcStorage.Close()
		p.grpcStorage = nil
	}
}

func (p *Runtime) startGRPC() error {
	options, err := runtimegrpc.TakeRuntimeEnv()
	if err != nil {
		return err
	}
	if client, clientErr := runtimegrpc.NewHostResourceClient(options.HostEndpoint, options.RuntimeToHostToken); clientErr == nil {
		p.grpcResources = client
	}
	if platform, platformErr := runtimegrpc.NewHostPlatformClient(options.HostEndpoint, options.RuntimeToHostToken); platformErr == nil {
		p.grpcPlatform = platform
	}
	if storage, storageErr := runtimegrpc.NewHostBrickStorageClient(options.HostEndpoint, options.RuntimeToHostToken); storageErr == nil {
		p.grpcStorage = storage
	}
	p.mu.RLock()
	commands := make([]string, 0, len(p.commandHandlers))
	for commandID := range p.commandHandlers {
		commands = append(commands, commandID)
	}
	p.mu.RUnlock()
	options.Commands = commands
	options.Invoke = func(rpcCtx context.Context, commandID string, input *runtimegrpc.BrickValue, invocationID string) (*runtimegrpc.BrickValue, error) {
		p.mu.RLock()
		handler, ok := p.commandHandlers[commandID]
		p.mu.RUnlock()
		if !ok {
			if commandID == "echo" {
				return input, nil
			}
			return nil, fmt.Errorf("unknown command: %s", commandID)
		}
		raw, convErr := runtimegrpc.BrickValueToJSON(input)
		if convErr != nil {
			return nil, convErr
		}
		requestID := firstNonEmpty(invocationID, "grpc-"+commandID)
		ctx := newCommandContext(p, requestID, commandID, CommandInvocationContext{Source: "unknown"}, nil, rpcCtx)
		p.enterCommand(requestID, ctx.Context())
		defer p.leaveCommand()
		result, invokeErr := handler(ctx, raw)
		if invokeErr != nil {
			return nil, invokeErr
		}
		prepared, prepErr := prepareResourceValue(result)
		if prepErr != nil {
			return nil, prepErr
		}
		return runtimegrpc.AnyToBrickValue(prepared)
	}
	options.Interact = func(commandID string, session runtimegrpc.InteractSession) (any, error) {
		p.mu.RLock()
		handler, ok := p.commandHandlers[commandID]
		p.mu.RUnlock()
		if !ok {
			return nil, fmt.Errorf("unknown command: %s", commandID)
		}
		raw, convErr := json.Marshal(session.Initial())
		if convErr != nil {
			return nil, convErr
		}
		requestID := firstNonEmpty(invocationIDFromContext(session.Context()), "grpc-"+commandID)
		ctx := newCommandContext(p, requestID, commandID, CommandInvocationContext{Source: "unknown"}, nil, session.Context())
		p.enterCommand(requestID, ctx.Context())
		defer p.leaveCommand()
		ctx.stream = bindInteractStream(session.Send, session.Events())
		result, interactErr := handler(ctx, raw)
		if interactErr != nil {
			return nil, interactErr
		}
		return result, nil
	}
	handle, err := runtimegrpc.StartRuntime(options)
	if err != nil {
		return err
	}
	p.grpcHandle = handle
	p.mu.RLock()
	fn := p.readyHandler
	p.mu.RUnlock()
	p.attachEventSubs()
	if fn != nil {
		go func() {
			defer func() {
				if r := recover(); r != nil {
					p.Error("onReady panic", fmt.Errorf("%v", r), nil)
				}
			}()
			if readyErr := fn(); readyErr != nil {
				p.Error("onReady error", readyErr, nil)
			}
		}()
	}
	return nil
}

// Invoke 再跑自己的一条命令。已有占用则不 Dispose。没有当前命令时是 root。
func (p *Runtime) Invoke(commandID string, input any) (any, error) {
	var out any
	if err := p.platformCall("runtime.invoke", map[string]any{
		"commandId": commandID,
		"input":     input,
	}, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// Interact 已有占用上再开会话，不 Dispose。没有占用则拒绝。
func (p *Runtime) Interact(ctx context.Context, commandID string, input any, opts ...InteractOptions) (Interaction, error) {
	options, err := requireInteractOnEvent(opts)
	if err != nil {
		return nil, err
	}
	session, err := p.platformInteract(ctx, commandID, input, "")
	if err != nil {
		return nil, err
	}
	pumpSessionEvents(session, options.OnEvent)
	return session, nil
}

// Call 是 interact + 半关闭的糖。必须与命令 mode=call 对齐。
func (p *Runtime) Call(ctx context.Context, commandID string, input any, opts ...CallOptions) (any, error) {
	return Call(ctx, runtimeSelfClient{p}, commandID, input, opts...)
}

type runtimeSelfClient struct{ runtime *Runtime }

func (c runtimeSelfClient) Invoke(_ context.Context, command string, input any) (any, error) {
	return c.runtime.Invoke(command, input)
}

func (c runtimeSelfClient) Interact(ctx context.Context, command string, input any, opts ...InteractOptions) (Interaction, error) {
	options, err := requireInteractOnEvent(opts)
	if err != nil {
		return nil, err
	}
	session, err := c.runtime.platformInteract(ctx, command, input, "call")
	if err != nil {
		return nil, err
	}
	pumpSessionEvents(session, options.OnEvent)
	return session, nil
}

func (p *Runtime) platformInteract(ctx context.Context, commandID string, input any, intent string) (Interaction, error) {
	if p.grpcPlatform == nil {
		return nil, NewBppError("PROTOCOL_ERROR", "PlatformService 未连接；gRPC Runtime 是唯一路径")
	}
	return p.grpcPlatform.PlatformInteract(p.outboundContext(ctx), commandID, input, p.currentInvocationID(), intent)
}

func (p *Runtime) platformCall(method string, input any, into any) error {
	if p.grpcPlatform == nil {
		return NewBppError("PROTOCOL_ERROR", "PlatformService 未连接；gRPC Runtime 是唯一路径")
	}
	ctx := context.Background()
	if id := p.currentInvocationID(); id != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, runtimegrpc.InvocationIdMD, id)
	}
	result, err := p.grpcPlatform.PlatformCall(ctx, method, input)
	if err != nil {
		return err
	}
	return runtimegrpc.AssignJSON(result, into)
}

func (p *Runtime) attachEventSubs() {
	for _, topic := range windowHostEventTopics {
		p.ensureEventSub(topic)
	}
	p.Events.mu.RLock()
	topics := make([]string, 0, len(p.Events.subs))
	for topic := range p.Events.subs {
		topics = append(topics, topic)
	}
	p.Events.mu.RUnlock()
	for _, topic := range topics {
		p.ensureEventSub(topic)
	}
}

func (p *Runtime) ensureEventSub(topic string) {
	if p.grpcPlatform == nil {
		return
	}
	p.eventSubsMu.Lock()
	defer p.eventSubsMu.Unlock()
	if p.eventSubs == nil {
		p.eventSubs = make(map[string]func())
	}
	if _, ok := p.eventSubs[topic]; ok {
		return
	}
	p.eventSubs[topic] = p.grpcPlatform.Subscribe(topic, p.handleDomainEvent)
}

func (p *Runtime) dropEventSub(topic string) {
	if strings.HasPrefix(topic, "window.") {
		return
	}
	p.eventSubsMu.Lock()
	defer p.eventSubsMu.Unlock()
	cancel, ok := p.eventSubs[topic]
	if !ok {
		return
	}
	cancel()
	delete(p.eventSubs, topic)
}

func (p *Runtime) clearEventSubs() {
	p.eventSubsMu.Lock()
	subs := p.eventSubs
	p.eventSubs = nil
	p.eventSubsMu.Unlock()
	for _, cancel := range subs {
		cancel()
	}
}

func (p *Runtime) eventSubCount() int {
	p.eventSubsMu.Lock()
	defer p.eventSubsMu.Unlock()
	return len(p.eventSubs)
}

func (p *Runtime) handleDomainEvent(topic string, payload any) {
	raw := map[string]any{"event": topic, "payload": payload}
	if fields, ok := payload.(map[string]any); ok {
		if requestID, ok := fields["requestId"].(string); ok {
			raw["requestId"] = requestID
		}
	}
	p.handleEventNotify(rawMessage{Type: "event.notify", Raw: raw})
}

func (p *Runtime) publishEvent(topic string, payload any) error {
	if p.grpcPlatform == nil {
		return NewBppError("PROTOCOL_ERROR", "EventService 未连接；gRPC Runtime 是唯一路径")
	}
	return p.grpcPlatform.Publish(context.Background(), topic, payload)
}

func (p *Runtime) connectorInvoke(brickID, commandID string, input any, invocationID string, into any) error {
	if p.grpcPlatform == nil {
		return NewBppError("PROTOCOL_ERROR", "Host Connector 未连接")
	}
	result, err := p.grpcPlatform.Connect(p.outboundContext(nil), brickID, commandID, input, invocationID)
	if err != nil {
		return err
	}
	return runtimegrpc.AssignJSON(result, into)
}

func (p *Runtime) connectorInvokeOnHandle(brickID, commandID string, input any, invocationID, handleID string, into any) error {
	if p.grpcPlatform == nil {
		return NewBppError("PROTOCOL_ERROR", "Host Connector 未连接")
	}
	result, err := p.grpcPlatform.ConnectOnHandle(p.outboundContext(nil), brickID, commandID, input, invocationID, handleID)
	if err != nil {
		return err
	}
	return runtimegrpc.AssignJSON(result, into)
}

func (p *Runtime) connectorInteract(ctx context.Context, brickID, commandID string, input any, invocationID, intent string) (Interaction, error) {
	if p.grpcPlatform == nil {
		return nil, NewBppError("PROTOCOL_ERROR", "Host Connector 未连接")
	}
	return p.grpcPlatform.Interact(p.outboundContext(ctx), brickID, commandID, input, invocationID, intent)
}

func (p *Runtime) connectorInteractOnHandle(ctx context.Context, brickID, commandID string, input any, invocationID, handleID, intent string) (Interaction, error) {
	if p.grpcPlatform == nil {
		return nil, NewBppError("PROTOCOL_ERROR", "Host Connector 未连接")
	}
	return p.grpcPlatform.InteractOnHandle(p.outboundContext(ctx), brickID, commandID, input, invocationID, handleID, intent)
}

func (p *Runtime) startDependency(_alias string, ref BrickRef) (*StartedToolHandle, error) {
	if !p.inCommandScope() || p.currentInvocationID() == "" {
		return nil, parentInvocationRequired(StartRequiresCommand)
	}
	if p.grpcPlatform == nil {
		return nil, NewBppError("PROTOCOL_ERROR", "Host Connector 未连接")
	}
	invocationID := p.currentInvocationID()
	handleID, err := p.grpcPlatform.StartDependency(p.outboundContext(nil), ref.BrickID, invocationID)
	if err != nil {
		return nil, err
	}
	return &StartedToolHandle{
		runtime:      p,
		ref:          ref,
		handleID:     handleID,
		invocationID: invocationID,
	}, nil
}

func (p *Runtime) disposeStarted(handleID, invocationID string, stop bool) error {
	if p.grpcPlatform == nil {
		return NewBppError("PROTOCOL_ERROR", "Host Connector 未连接")
	}
	return p.grpcPlatform.DisposeDependency(context.Background(), handleID, invocationID, stop)
}

// Debug 经 Host PlatformService `diagnostics.log` 进入日志中心。
func (p *Runtime) Debug(message string, fields map[string]any) {
	p.emitBrickLog("debug", message, nil, fields, p.currentInvocationID())
}

// Info 经 Host PlatformService `diagnostics.log` 进入日志中心。
func (p *Runtime) Info(message string, fields map[string]any) {
	p.emitBrickLog("info", message, nil, fields, p.currentInvocationID())
}

// Warn 经 Host PlatformService `diagnostics.log` 进入日志中心。
func (p *Runtime) Warn(message string, fields map[string]any) {
	p.emitBrickLog("warn", message, nil, fields, p.currentInvocationID())
}

// Error 经 Host PlatformService `diagnostics.log` 进入日志中心。
func (p *Runtime) Error(message string, err error, fields map[string]any) {
	p.emitBrickLog("error", message, err, fields, p.currentInvocationID())
}

func (p *Runtime) enterCommand(id string, cmdCtx context.Context) {
	p.inCommand.Store(true)
	p.setCurrentRequestID(id)
	if cmdCtx == nil {
		cmdCtx = context.Background()
	}
	p.currentCommandCtx.Store(storedCommandCtx{ctx: cmdCtx})
}

func (p *Runtime) leaveCommand() {
	p.inCommand.Store(false)
	p.setCurrentRequestID("")
	p.currentCommandCtx.Store(storedCommandCtx{})
}

func (p *Runtime) inCommandScope() bool {
	return p.inCommand.Load()
}

func (p *Runtime) setCurrentRequestID(id string) {
	p.currentRequestID.Store(id)
}

func (p *Runtime) currentInvocationID() string {
	id, _ := p.currentRequestID.Load().(string)
	return id
}

func (p *Runtime) outboundContext(explicit context.Context) context.Context {
	box, _ := p.currentCommandCtx.Load().(storedCommandCtx)
	cmd := box.ctx
	if cmd == nil {
		if explicit != nil {
			return explicit
		}
		return context.Background()
	}
	if explicit == nil || explicit == context.Background() || explicit == cmd {
		return cmd
	}
	ctx, cancel := context.WithCancel(cmd)
	go func() {
		select {
		case <-explicit.Done():
			cancel()
		case <-ctx.Done():
		}
	}()
	return ctx
}

func invocationIDFromContext(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	values := md.Get(runtimegrpc.InvocationIdMD)
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

type storedCommandCtx struct {
	ctx context.Context
}

func (p *Runtime) trackEventRequest(id string, cancel func()) {
	if id == "" {
		return
	}
	p.cancelMu.Lock()
	p.cancelHandlers[id] = cancel
	p.cancelMu.Unlock()
	p.setCurrentRequestID(id)
}

func (p *Runtime) untrackEventRequest(id string) {
	if id == "" {
		return
	}
	p.cancelMu.Lock()
	delete(p.cancelHandlers, id)
	p.cancelMu.Unlock()
}

func (p *Runtime) emitBrickLog(level, message string, err error, fields map[string]any, invocationID string) {
	if p.grpcPlatform == nil {
		return
	}
	payload := map[string]any{
		"level":        level,
		"message":      message,
		"fields":       fields,
		"invocationId": invocationID,
	}
	if err != nil {
		payload["error"] = err.Error()
	}
	go func() {
		_, _ = p.grpcPlatform.PlatformCall(context.Background(), "diagnostics.log", payload)
	}()
}

// eventNotify 是 EventBus 本地投递签名，不是线上协议帧。
type rawMessage struct {
	Type string
	Raw  map[string]any
}

func (p *Runtime) handleEventNotify(msg rawMessage) {
	event, _ := msg.Raw["event"].(string)
	payloadRaw := msg.Raw["payload"]
	if event == "window.closed" {
		if payload, ok := payloadRaw.(map[string]any); ok {
			if eventID, ok := payload["eventId"].(string); ok && !p.rememberTerminalWindowEvent(eventID) {
				return
			}
		}
	}

	// 路由 window.* 事件到具体 WindowHandle
	if strings.HasPrefix(event, "window.") {
		if m, ok := payloadRaw.(map[string]any); ok {
			if widF, ok := m["windowId"].(float64); ok {
				wid := int64(widF)
				p.windowsMu.RLock()
				handle := p.windows[wid]
				p.windowsMu.RUnlock()
				if handle != nil {
					name := strings.TrimPrefix(event, "window.")
					if name == "notify" || name == "request" || name == "request.cancel" {
						handle.dispatchChildRPC(name, m)
					} else {
						handle.emit(name, m)
					}
				}
			}
		}
	}

	if !strings.HasPrefix(event, "window.") {
		p.Events.dispatch(event, payloadRaw, msg.Raw)
	}
}

func (p *Runtime) signalDone() {
	p.clearWindows()
	p.doneOnce.Do(func() { close(p.done) })
}

func (p *Runtime) removeWindow(windowID int64, expected *WindowHandle) {
	p.windowsMu.Lock()
	if p.windows[windowID] == expected {
		delete(p.windows, windowID)
	}
	p.windowsMu.Unlock()
}

func (p *Runtime) clearWindows() {
	p.windowsMu.Lock()
	handles := make([]*WindowHandle, 0, len(p.windows))
	for _, handle := range p.windows {
		handles = append(handles, handle)
	}
	p.windows = make(map[int64]*WindowHandle)
	p.windowsMu.Unlock()
	for _, handle := range handles {
		handle.disposeLocal("")
	}
}

func (p *Runtime) rememberTerminalWindowEvent(eventID string) bool {
	p.terminalWindowEventsMu.Lock()
	defer p.terminalWindowEventsMu.Unlock()
	if _, exists := p.terminalWindowEventIDs[eventID]; exists {
		return false
	}
	p.terminalWindowEventIDs[eventID] = struct{}{}
	p.terminalWindowOrder = append(p.terminalWindowOrder, eventID)
	if len(p.terminalWindowOrder) > maxTerminalWindowEventIDs {
		oldest := p.terminalWindowOrder[0]
		p.terminalWindowOrder = p.terminalWindowOrder[1:]
		delete(p.terminalWindowEventIDs, oldest)
	}
	return true
}

// —— 小工具 ——

func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}
