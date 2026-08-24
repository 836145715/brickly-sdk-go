// Package brickly 是 Brickly Brick Go runtime 官方 SDK。
//
// 与 @syllm/brickly-sdk (Node) 保持一致的 API 表面与语义：
//
//	p := brickly.New(brickly.Options{BrickID: "com.example.foo"})
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

	runtimegrpc "github.com/836145715/brickly-sdk-go/internal/grpc"
)

// Options 是 New 的可选配置。
type Options struct {
	// 必填：Brick id，仅用于生成协议请求 id 前缀；Runtime 身份由宿主 manifest 决定。
	BrickID string
	// 可选：协议版本。默认 ProtocolVersion。
	ProtocolVersion string
}

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
	brickID         string
	protocolVersion string

	// UI / Events / Platform 与 Node SDK 的 brick.ui / brick.events /
	// brick.platform 同源。System 是 Platform.System 的便捷别名。
	UI           *UI
	Events       *EventBus
	Platform     *PlatformAPI
	System       *SystemAPI
	Dependencies *DependencyRegistry
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

	started       atomic.Bool
	done          chan struct{}
	doneOnce      sync.Once
	grpcHandle    *runtimegrpc.RuntimeHandle
	grpcResources *runtimegrpc.HostResourceClient
	grpcPlatform  *runtimegrpc.HostPlatformClient
}

// New 创建并返回一个 Runtime 实例。不会连接 Host——那是 Start 的职责。
func New(opts Options) *Runtime {
	if opts.BrickID == "" {
		panic("brickly: Options.BrickID is required")
	}
	p := &Runtime{
		brickID:                opts.BrickID,
		protocolVersion:        firstNonEmpty(opts.ProtocolVersion, ProtocolVersion),
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
	if p.grpcPlatform != nil {
		_ = p.grpcPlatform.Close()
		p.grpcPlatform = nil
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
	p.mu.RLock()
	commands := make([]string, 0, len(p.commandHandlers))
	for commandID := range p.commandHandlers {
		commands = append(commands, commandID)
	}
	p.mu.RUnlock()
	options.Commands = commands
	options.Invoke = func(commandID string, input *runtimegrpc.BrickValue) (*runtimegrpc.BrickValue, error) {
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
		ctx := newCommandContext(p, "grpc-"+commandID, commandID, CommandInvocationContext{Source: "unknown"}, nil)
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
		ctx := newCommandContext(p, "grpc-"+commandID, commandID, CommandInvocationContext{Source: "unknown"}, nil)
		ctx.stream = bindInteractStream(session.Send, session.Events())
		go func() {
			select {
			case <-session.Context().Done():
				ctx.cancel()
			case <-ctx.Context().Done():
			}
		}()
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

func (p *Runtime) platformCall(method string, input any, into any) error {
	if p.grpcPlatform == nil {
		return NewBppError("PROTOCOL_ERROR", "PlatformService 未连接；gRPC Runtime 是唯一路径")
	}
	result, err := p.grpcPlatform.PlatformCall(context.Background(), method, input)
	if err != nil {
		return err
	}
	return runtimegrpc.AssignJSON(result, into)
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
	result, err := p.grpcPlatform.Connect(context.Background(), brickID, commandID, input, invocationID)
	if err != nil {
		return err
	}
	return runtimegrpc.AssignJSON(result, into)
}

func (p *Runtime) connectorInteract(ctx context.Context, brickID, commandID string, input any, invocationID string) (Interaction, error) {
	if p.grpcPlatform == nil {
		return nil, NewBppError("PROTOCOL_ERROR", "Host Connector 未连接")
	}
	return p.grpcPlatform.Interact(ctx, brickID, commandID, input, invocationID)
}

// Debug 结构化日志入口（当前为 no-op，保留与 Node/Python API 对齐）。
func (p *Runtime) Debug(string, map[string]any) {}

// Info 结构化日志入口（当前为 no-op，保留与 Node/Python API 对齐）。
func (p *Runtime) Info(string, map[string]any) {}

// Warn 结构化日志入口（当前为 no-op，保留与 Node/Python API 对齐）。
func (p *Runtime) Warn(string, map[string]any) {}

// Error 结构化日志入口（当前为 no-op，保留与 Node/Python API 对齐）。
func (p *Runtime) Error(string, error, map[string]any) {}

// eventNotify 是 EventBus 本地投递签名，不是线上协议帧。
type rawMessage struct {
	Type string
	Raw  map[string]any
}

// BrickID 返回当前 Brick id。
func (p *Runtime) BrickID() string { return p.brickID }

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
					handle.emit(strings.TrimPrefix(event, "window."), m)
				}
			}
		}
	}

	p.Events.dispatch(event, unwrapEventResource(payloadRaw), msg.Raw)
}

func unwrapEventResource(value any) any {
	if envelope, ok := value.(map[string]any); ok && envelope["encoding"] == "json" {
		if resource, ok := hydrateResourceValue(envelope["resource"], 0).(*ResourceHandle); ok {
			return resource
		}
	}
	return value
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
