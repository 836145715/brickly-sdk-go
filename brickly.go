// Package brickly 是 Brickly Brick Go runtime 官方 SDK。
//
// 与 @syllm/brickly-sdk (Node) 保持一致的 API 表面与语义：
//
//	p := brickly.New(brickly.Options{BrickID: "com.example.foo"})
//	p.OnCommand("hello", func(ctx *brickly.CommandContext, input json.RawMessage) (any, error) {
//	    return map[string]any{"ok": true}, nil
//	})
//	p.Start() // 阻塞，直到 stdin 关闭或收到 runtime.shutdown
//
// 零外部依赖，只使用 Go 标准库。
// stdout 仅写 BPP 协议；业务日志必须用 Info/Warn/Error/Debug（runtime.log），禁止无 level 的 stderr 业务输出。
package brickly

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"context"
)

// Options 是 New 的可选配置。
type Options struct {
	// 必填：Brick id，用于协议请求 id 与 runtime.ready 身份（要与 manifest.json 的 id 一致）。
	BrickID string
	// 可选：协议版本。默认 ProtocolVersion。
	ProtocolVersion string
	// 可选：stdin / stdout / stderr 流，默认取进程标准流。测试时可注入 pipe。
	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer
}

// CommandHandler 是 OnCommand 注册的业务处理函数。
//
// 返回 (result, nil) 时 SDK 自动发送 command.result；
// 返回 (_, err) 时 SDK 自动发送 command.error（*BppError 会保留其 code）。
type CommandHandler func(ctx *CommandContext, input json.RawMessage) (any, error)

// ReadyHandler 在 SDK 回发 runtime.ready 之后异步触发。
type ReadyHandler func() error

// ShutdownHandler 在收到 runtime.shutdown 时触发；返回后 SDK 自动发 runtime.bye 并退出。
type ShutdownHandler func() error

const maxTerminalWindowEventIDs = 1024

// Runtime 是 SDK 主入口，通过 New 创建。
type Runtime struct {
	brickID         string
	protocolVersion string

	transport *transport

	// UI / Events / Platform 与 Node SDK 的 brick.ui / brick.events /
	// brick.platform 同源。System 是 Platform.System 的便捷别名。
	UI       *UI
	Events   *EventBus
	Platform *PlatformAPI
	System   *SystemAPI
	Config   map[string]any

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

	started  atomic.Bool
	done     chan struct{}
	doneOnce sync.Once
}

// New 创建并返回一个 Runtime 实例。不会启动 stdin 循环——那是 Start 的职责。
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
	p.transport = newTransport(transportOptions{
		brickID:   opts.BrickID,
		stdin:     firstReader(opts.Stdin, os.Stdin),
		stdout:    firstWriter(opts.Stdout, os.Stdout),
		stderr:    firstWriter(opts.Stderr, os.Stderr),
		onMessage: p.dispatch,
		onEnd:     p.signalDone,
	})
	p.UI = &UI{runtime: p}
	p.Events = &EventBus{runtime: p}
	p.System = &SystemAPI{runtime: p}
	p.Platform = &PlatformAPI{System: p.System, Clipboard: &ClipboardAPI{runtime: p}}
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

// OnReady 注册 ready 钩子（runtime.ready 回发后异步触发，适合 host-start service 或实例初始化逻辑）。
func (p *Runtime) OnReady(fn ReadyHandler) *Runtime {
	p.mu.Lock()
	p.readyHandler = fn
	p.mu.Unlock()
	return p
}

// OnShutdown 注册 shutdown 钩子；返回后 SDK 自动发 runtime.bye 并退出进程。
func (p *Runtime) OnShutdown(fn ShutdownHandler) *Runtime {
	p.mu.Lock()
	p.shutdownHandler = fn
	p.mu.Unlock()
	return p
}

// Start 启动 stdin 循环并阻塞，直到 stdin 关闭或收到 runtime.shutdown。
// 典型用法：main 函数最后一行调用。
func (p *Runtime) Start() {
	if !p.started.CompareAndSwap(false, true) {
		return
	}
	p.transport.start()
	<-p.done
}

// Debug 发送 debug 级别结构化日志到宿主（runtime.log）。
func (p *Runtime) Debug(message string, fields map[string]any) {
	p.transport.sendLog("debug", message, fields, nil, nil)
}

// Info 发送 info 级别结构化日志到宿主（runtime.log）。
func (p *Runtime) Info(message string, fields map[string]any) {
	p.transport.sendLog("info", message, fields, nil, nil)
}

// Warn 发送 warn 级别结构化日志到宿主（runtime.log）。
func (p *Runtime) Warn(message string, fields map[string]any) {
	p.transport.sendLog("warn", message, fields, nil, nil)
}

// Error 发送 error 级别结构化日志到宿主（runtime.log）。
func (p *Runtime) Error(message string, err error, fields map[string]any) {
	var errPayload map[string]any
	if err != nil {
		errPayload = map[string]any{"code": "BRICK_ERROR", "message": err.Error()}
	}
	p.transport.sendLog("error", message, fields, errPayload, nil)
}

// BrickID 返回当前 Brick id。
func (p *Runtime) BrickID() string { return p.brickID }

// —— 内部：消息分发（由 transport 在非 host.*/ping 消息到达时回调） ——

func (p *Runtime) dispatch(msg rawMessage) {
	switch msg.Type {
	case "host.hello":
		p.handleHello(msg)
	case "command.invoke":
		p.handleInvoke(msg)
	case "command.cancel":
		p.handleCancel(msg)
	case "event.notify":
		p.handleEventNotify(msg)
	case "runtime.shutdown":
		p.handleShutdown()
	default:
		p.Warn("unknown BPP message type", map[string]any{"type": msg.Type})
	}
}

func (p *Runtime) handleHello(msg rawMessage) {
	if config, ok := msg.Raw["config"].(map[string]any); ok {
		p.mu.Lock()
		p.Config = config
		p.mu.Unlock()
	}
	p.transport.send(map[string]any{
		"type":            "runtime.ready",
		"protocolVersion": p.protocolVersion,
		"brickId":         p.brickID,
	})
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
			if err := fn(); err != nil {
				p.Error("onReady error", err, nil)
			}
		}()
	}
}

func (p *Runtime) handleInvoke(msg rawMessage) {
	reqID, _ := msg.Raw["id"].(string)
	commandID, _ := msg.Raw["commandId"].(string)

	var input json.RawMessage
	if raw, ok := msg.Raw["input"]; ok {
		input, _ = json.Marshal(raw)
	}

	p.mu.RLock()
	handler, ok := p.commandHandlers[commandID]
	p.mu.RUnlock()
	if !ok {
		p.transport.send(map[string]any{
			"type": "command.error",
			"id":   reqID,
			"error": map[string]any{
				"code":    "COMMAND_NOT_FOUND",
				"message": "Unknown command: " + commandID,
			},
		})
		return
	}

	ctx := newCommandContext(p, reqID, commandID, extractTrace(msg))
	go func() {
		defer p.finishInvoke(reqID)

		var result any
		var err error
		func() {
			defer func() {
				if r := recover(); r != nil {
					err = fmt.Errorf("handler panic: %v", r)
				}
			}()
			result, err = handler(ctx, input)
		}()

		if err != nil {
			p.transport.send(map[string]any{
				"type":  "command.error",
				"id":    reqID,
				"error": errorToPayload(err),
			})
			return
		}
		out := map[string]any{"type": "command.result", "id": reqID}
		if result != nil {
			out["result"] = result
		}
		p.transport.send(out)
	}()
}

func (p *Runtime) finishInvoke(reqID string) {
	p.cancelMu.Lock()
	if cancel, ok := p.cancelHandlers[reqID]; ok {
		cancel()
		delete(p.cancelHandlers, reqID)
	}
	delete(p.cancelled, reqID)
	p.cancelMu.Unlock()
}

func (p *Runtime) handleCancel(msg rawMessage) {
	reqID, _ := msg.Raw["id"].(string)
	p.cancelMu.Lock()
	p.cancelled[reqID] = true
	cancel := p.cancelHandlers[reqID]
	p.cancelMu.Unlock()
	if cancel != nil {
		cancel()
	}
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
					handle.emit(strings.TrimPrefix(event, "window."), m)
				}
			}
		}
	}

	p.Events.dispatch(event, payloadRaw, msg.Raw)
}

func (p *Runtime) handleShutdown() {
	// 整个 shutdown 流程必须放到独立 goroutine，避免阻塞 readLoop——
	// 否则用户在 onShutdown 里调用 win.Close() 等 hostCall 会因 readLoop
	// 无法读 host.result 而死锁（与 event handler 同源问题）。
	go p.runShutdown()
}

func (p *Runtime) runShutdown() {
	p.clearWindows()
	p.mu.RLock()
	fn := p.shutdownHandler
	p.mu.RUnlock()
	if fn != nil {
		func() {
			defer func() {
				if r := recover(); r != nil {
					p.Error("onShutdown panic", fmt.Errorf("%v", r), nil)
				}
			}()
			if err := fn(); err != nil {
				p.Error("onShutdown error", err, nil)
			}
		}()
	}
	p.transport.send(map[string]any{"type": "runtime.bye"})
	p.transport.flush()
	// 留一点时间让字节流出
	time.Sleep(50 * time.Millisecond)
	p.signalDone()
	os.Exit(0)
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

func firstReader(a, b io.Reader) io.Reader {
	if a != nil {
		return a
	}
	return b
}

func firstWriter(a, b io.Writer) io.Writer {
	if a != nil {
		return a
	}
	return b
}
