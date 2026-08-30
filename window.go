package brickly

import (
	"encoding/json"
	"sync"
	"sync/atomic"
)

// Bounds 对应 setBounds / setContentBounds 的入参；全部字段可选。
type Bounds struct {
	X      *int `json:"x,omitempty"`
	Y      *int `json:"y,omitempty"`
	Width  *int `json:"width,omitempty"`
	Height *int `json:"height,omitempty"`
}

// Rect 对应 getBounds / getContentBounds / getNormalBounds 的返回值。
type Rect struct {
	X      int `json:"x"`
	Y      int `json:"y"`
	Width  int `json:"width"`
	Height int `json:"height"`
}

// Size 辅助类型，用于 SetAspectRatio 的 extraSize 参数。
type Size struct {
	Width  int `json:"width"`
	Height int `json:"height"`
}

// ProgressBarOptions 用于 SetProgressBar。
type ProgressBarOptions struct {
	// Mode: none|normal|indeterminate|error|paused
	Mode string `json:"mode,omitempty"`
}

// IgnoreMouseOptions 用于 SetIgnoreMouseEvents。
type IgnoreMouseOptions struct {
	Forward bool `json:"forward"`
}

// VisibleOnAllWorkspacesOptions 用于 SetVisibleOnAllWorkspaces。
type VisibleOnAllWorkspacesOptions struct {
	VisibleOnFullScreen bool `json:"visibleOnFullScreen"`
}

// OpenDevToolsOptions 用于 WebContents.OpenDevTools。
type OpenDevToolsOptions struct {
	// Mode: left|right|bottom|undocked|detach
	Mode string `json:"mode,omitempty"`
}

// WindowHandle 是一个子窗口句柄，封装 ui.window.call 反射方法。
//
//   - 所有 setXxx / 动作方法返回 error。
//   - 所有 getXxx / isXxx 返回 (值, error)。
//   - 关闭后再调用（除 IsDestroyed）会立即返回 INVALID_INPUT。
//
// 事件通过 On("closed" / "focus" / "blur" / "resize" / ...) 订阅。
type WindowHandle struct {
	ID            int64
	WindowKey     string
	WebContentsID int64
	URL           string

	runtime *Runtime

	mu           sync.Mutex
	closed       bool
	closeAttempt *windowCloseAttempt
	forceAttempt *windowForceAttempt

	handlersMu sync.RWMutex
	handlers   map[string]map[uint64]func(payload map[string]any)
	handlerSeq atomic.Uint64

	exposeMu  sync.RWMutex
	exposed   map[string]WindowExposeHandler
	inflight  map[string]chan struct{}
	lastReply map[string]any
}

// WindowExposeSession 是子窗 request 进行中的会话。
type WindowExposeSession struct {
	Emit func(event any) error
	Done <-chan struct{}
}

// WindowExposeHandler 处理子窗 notify/request。
type WindowExposeHandler func(payload any, session WindowExposeSession) (any, error)

type windowCloseAttempt struct {
	done   chan struct{}
	result WindowRequestCloseResult
	err    error
}

type windowForceAttempt struct {
	done   chan struct{}
	result WindowTerminationResult
	err    error
}

func newWindowHandle(p *Runtime, id int64) *WindowHandle {
	return &WindowHandle{
		ID:       id,
		runtime:  p,
		handlers: make(map[string]map[uint64]func(map[string]any)),
		exposed:  make(map[string]WindowExposeHandler),
		inflight: make(map[string]chan struct{}),
	}
}

func newWindowHandleFromResult(p *Runtime, key string, id, webContentsID int64, url string) *WindowHandle {
	handle := newWindowHandle(p, id)
	handle.WindowKey = key
	handle.WebContentsID = webContentsID
	handle.URL = url
	return handle
}

// Call 通用调用：走 ui.window.call 白名单。
//
// 通常你只需要用强类型方法（SetBounds / GetTitle / ...），但当宿主提前
// 开放了新方法、而 SDK 尚未升级时，可用 Call 直接调用。
func (w *WindowHandle) Call(method string, args []any, into any) error {
	return w.callWithParent(method, args, "", into)
}

func (w *WindowHandle) callWithParent(method string, args []any, parentRequestID string, into any) error {
	return w.callWithParentTrace(method, args, parentRequestID, nil, into)
}

func (w *WindowHandle) callWithParentTrace(method string, args []any, parentRequestID string, trace *TraceContext, into any) error {
	if method == "close" || method == "destroy" {
		return NewBppError("INVALID_INPUT", method+" is a lifecycle operation, not a reflected call")
	}
	w.mu.Lock()
	closed := w.closed
	runtime := w.runtime
	w.mu.Unlock()
	if closed && method == "isDestroyed" {
		if value, ok := into.(*bool); ok {
			*value = true
		}
		return nil
	}
	if closed {
		return NewBppError("INVALID_INPUT", "Window already closed")
	}
	if runtime == nil {
		return NewBppError("INVALID_INPUT", "Window already closed")
	}
	if args == nil {
		args = []any{}
	}
	if method == "webContents.send" && parentRequestID == "" {
		parentRequestID = explicitPayloadRequestID(args)
	}
	if method == "webContents.send" && parentRequestID == "" {
		return parentInvocationRequired("webContents.Send must run through CommandContext.UI or include payload.requestId")
	}
	msg := map[string]any{
		"windowId": w.ID,
		"method":   method,
		"args":     args,
	}
	if parentRequestID != "" {
		msg["parentRequestId"] = parentRequestID
	}
	return runtime.platformCall("ui.window.call", msg, into)
}

// Close 请求普通关闭；pending/prevented 时句柄保持可用。
func (w *WindowHandle) Close() (WindowRequestCloseResult, error) {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return WindowRequestCloseResult{Status: WindowCloseNotFound}, nil
	}
	if attempt := w.closeAttempt; attempt != nil {
		w.mu.Unlock()
		<-attempt.done
		return attempt.result, attempt.err
	}
	attempt := &windowCloseAttempt{done: make(chan struct{})}
	w.closeAttempt = attempt
	runtime := w.runtime
	w.mu.Unlock()
	if runtime == nil {
		err := NewBppError("INVALID_INPUT", "Window already closed")
		w.finishCloseAttempt(attempt, WindowRequestCloseResult{}, err)
		return WindowRequestCloseResult{}, err
	}

	var result WindowRequestCloseResult
	err := runtime.platformCall("ui.window.requestClose", map[string]any{
		"windowId": w.ID,
	}, &result)
	if err == nil && !validWindowCloseStatus(result.Status) {
		err = NewBppError("PROTOCOL_ERROR", "ui.window.requestClose returned an invalid result")
	}
	if err == nil && (result.Status == WindowCloseClosed || result.Status == WindowCloseNotFound) {
		w.disposeLocal("")
	}

	w.finishCloseAttempt(attempt, result, err)
	return result, err
}

// ForceClose 强制终止窗口，不经过页面关闭协商。
func (w *WindowHandle) ForceClose() (WindowTerminationResult, error) {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return WindowTerminationResult{}, NewBppError("INVALID_INPUT", "Window already closed")
	}
	if attempt := w.forceAttempt; attempt != nil {
		w.mu.Unlock()
		<-attempt.done
		return attempt.result, attempt.err
	}
	attempt := &windowForceAttempt{done: make(chan struct{})}
	w.forceAttempt = attempt
	runtime := w.runtime
	w.mu.Unlock()
	if runtime == nil {
		err := NewBppError("INVALID_INPUT", "Window already closed")
		w.finishForceAttempt(attempt, WindowTerminationResult{}, err)
		return WindowTerminationResult{}, err
	}

	var result WindowTerminationResult
	err := runtime.platformCall("ui.window.forceClose", map[string]any{
		"windowId": w.ID,
	}, &result)
	if err == nil && !validWindowTerminationResult(result) {
		err = NewBppError("PROTOCOL_ERROR", "ui.window.forceClose returned an invalid result")
	}
	if err == nil {
		w.disposeLocal("")
	}

	w.finishForceAttempt(attempt, result, err)
	return result, err
}

func (w *WindowHandle) finishCloseAttempt(attempt *windowCloseAttempt, result WindowRequestCloseResult, err error) {
	w.mu.Lock()
	attempt.result = result
	attempt.err = err
	if w.closeAttempt == attempt {
		w.closeAttempt = nil
	}
	close(attempt.done)
	w.mu.Unlock()
}

func (w *WindowHandle) finishForceAttempt(attempt *windowForceAttempt, result WindowTerminationResult, err error) {
	w.mu.Lock()
	attempt.result = result
	attempt.err = err
	if w.forceAttempt == attempt {
		w.forceAttempt = nil
	}
	close(attempt.done)
	w.mu.Unlock()
}

// IsClosed 返回本地记录的关闭状态（不发协议消息）。
// 如需权威状态请使用 IsDestroyed()。
func (w *WindowHandle) IsClosed() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.closed
}

// Expose 登记子窗可调用的方法。表跟窗走，不跟开窗 command 走。
func (w *WindowHandle) Expose(handlers map[string]WindowExposeHandler) error {
	w.exposeMu.Lock()
	defer w.exposeMu.Unlock()
	if w.exposed == nil {
		w.exposed = make(map[string]WindowExposeHandler)
	}
	for name, handler := range handlers {
		if len(name) >= 8 && name[:8] == "brickly:" {
			return NewBppError("RESERVED_NAME", "不能 expose 保留名 "+name)
		}
		w.exposed[name] = handler
	}
	return nil
}

// Send 向子窗推送，不要求 parentRequestId。
func (w *WindowHandle) Send(name string, payload any) error {
	if len(name) >= 8 && name[:8] == "brickly:" {
		return NewBppError("RESERVED_NAME", "不能推送保留名 "+name)
	}
	if w.IsClosed() {
		return NewBppError("INVALID_INPUT", "window already closed")
	}
	return w.runtime.platformCall("ui.window.send", map[string]any{
		"windowId": w.ID,
		"name":     name,
		"payload":  payload,
	}, nil)
}

func (w *WindowHandle) dispatchChildRPC(event string, payload map[string]any) {
	switch event {
	case "notify":
		name, _ := payload["name"].(string)
		w.exposeMu.RLock()
		handler := w.exposed[name]
		w.exposeMu.RUnlock()
		if handler == nil {
			return
		}
		done := make(chan struct{})
		close(done)
		_, _ = handler(payload["payload"], WindowExposeSession{Emit: func(any) error { return nil }, Done: done})
	case "request":
		w.dispatchChildRequest(payload)
	case "request.cancel":
		requestID, _ := payload["requestId"].(string)
		w.exposeMu.Lock()
		if ch := w.inflight[requestID]; ch != nil {
			select {
			case <-ch:
			default:
				close(ch)
			}
			delete(w.inflight, requestID)
		}
		w.exposeMu.Unlock()
	}
}

func (w *WindowHandle) dispatchChildRequest(payload map[string]any) {
	name, _ := payload["name"].(string)
	requestID, _ := payload["requestId"].(string)
	if requestID == "" {
		return
	}
	w.exposeMu.RLock()
	handler := w.exposed[name]
	w.exposeMu.RUnlock()
	if handler == nil {
		w.childReply(requestID, false, nil, "NOT_EXPOSED", "未 expose \""+name+"\"")
		return
	}
	done := make(chan struct{})
	w.exposeMu.Lock()
	w.inflight[requestID] = done
	w.exposeMu.Unlock()
	go func() {
		if w.runtime != nil {
			w.runtime.trackEventRequest(requestID, func() {
				select {
				case <-done:
				default:
					close(done)
				}
			})
		}
		defer func() {
			if w.runtime != nil {
				w.runtime.untrackEventRequest(requestID)
			}
			w.exposeMu.Lock()
			delete(w.inflight, requestID)
			w.exposeMu.Unlock()
		}()
		result, err := handler(payload["payload"], WindowExposeSession{
			Emit: func(event any) error {
				return w.optionalPlatformCall("ui.window.emit", map[string]any{
					"windowId":  w.ID,
					"requestId": requestID,
					"event":     event,
				})
			},
			Done: done,
		})
		if err != nil {
			code, message := "INTERNAL_ERROR", err.Error()
			if bpp, ok := err.(*BppError); ok {
				code, message = bpp.Code, bpp.Message
			}
			w.childReply(requestID, false, nil, code, message)
			return
		}
		w.childReply(requestID, true, result, "", "")
	}()
}

func (w *WindowHandle) childReply(requestID string, ok bool, result any, code, message string) {
	reply := map[string]any{"ok": ok, "result": result}
	if !ok {
		reply["error"] = map[string]any{"code": code, "message": message}
	}
	w.exposeMu.Lock()
	w.lastReply = map[string]any{"requestId": requestID, "reply": reply}
	w.exposeMu.Unlock()
	_ = w.optionalPlatformCall("ui.window.reply", map[string]any{
		"windowId":  w.ID,
		"requestId": requestID,
		"ok":        ok,
		"result":    result,
		"error":     reply["error"],
	})
}

func (w *WindowHandle) optionalPlatformCall(method string, input any) error {
	if w.runtime == nil || w.runtime.grpcPlatform == nil {
		return nil
	}
	return w.runtime.platformCall(method, input, nil)
}

// On 订阅窗口生命周期事件，返回取消订阅函数。
//
// event 取值与宿主线上主题后半段一致：
// closed / focus / blur / resize / move / show / hide。
func (w *WindowHandle) On(event string, fn func(payload map[string]any)) func() {
	id := w.handlerSeq.Add(1)
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return func() {}
	}
	w.handlersMu.Lock()
	if w.handlers[event] == nil {
		w.handlers[event] = make(map[uint64]func(map[string]any))
	}
	w.handlers[event][id] = fn
	w.handlersMu.Unlock()
	w.mu.Unlock()
	return func() {
		w.handlersMu.Lock()
		defer w.handlersMu.Unlock()
		if m := w.handlers[event]; m != nil {
			delete(m, id)
			if len(m) == 0 {
				delete(w.handlers, event)
			}
		}
	}
}

func (w *WindowHandle) emit(event string, payload map[string]any) {
	if event == "closed" {
		w.dispatchHandlers(w.disposeLocal("closed"), payload)
		return
	}
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return
	}
	w.handlersMu.RLock()
	m := w.handlers[event]
	fns := make([]func(map[string]any), 0, len(m))
	for _, fn := range m {
		fns = append(fns, fn)
	}
	w.handlersMu.RUnlock()
	w.mu.Unlock()
	w.dispatchHandlers(fns, payload)
}

func (w *WindowHandle) dispatchHandlers(fns []func(map[string]any), payload map[string]any) {
	// 同 EventBus.dispatch：在 goroutine 中触发，避免 readLoop 自死锁。
	for _, fn := range fns {
		go func(f func(map[string]any)) {
			defer func() { _ = recover() }()
			f(payload)
		}(fn)
	}
}

func (w *WindowHandle) disposeLocal(terminalEvent string) []func(map[string]any) {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return nil
	}
	w.closed = true
	runtime := w.runtime
	if runtime != nil {
		runtime.removeWindow(w.ID, w)
	}
	w.runtime = nil
	w.handlersMu.Lock()
	fns := make([]func(map[string]any), 0)
	if terminalEvent != "" {
		for _, fn := range w.handlers[terminalEvent] {
			fns = append(fns, fn)
		}
	}
	w.handlers = make(map[string]map[uint64]func(map[string]any))
	w.handlersMu.Unlock()
	w.exposeMu.Lock()
	w.exposed = make(map[string]WindowExposeHandler)
	for _, ch := range w.inflight {
		select {
		case <-ch:
		default:
			close(ch)
		}
	}
	w.inflight = make(map[string]chan struct{})
	w.exposeMu.Unlock()
	w.mu.Unlock()
	return fns
}

func validWindowCloseStatus(status WindowCloseStatus) bool {
	return status == WindowCloseClosed || status == WindowClosePrevented ||
		status == WindowClosePending || status == WindowCloseNotFound
}

func validWindowTerminationResult(result WindowTerminationResult) bool {
	validEvent := result.Event == WindowTerminalEventSent || result.Event == WindowTerminalEventSkipped || result.Event == WindowTerminalEventFailed
	validWindow := result.Window == WindowNativeDestroyed || result.Window == WindowNativeAlreadyDestroyed || result.Window == WindowNativeFailed
	return validEvent && validWindow && result.Errors != nil
}

// —— 以下是窗口反射方法的强类型包装，按 specs/window-api.md 分组 ——

// 1. 几何 / 位置（17）=====================================================

func (w *WindowHandle) SetBounds(b Bounds) error { return w.Call("setBounds", []any{b}, nil) }
func (w *WindowHandle) GetBounds() (Rect, error) {
	var r Rect
	err := w.Call("getBounds", nil, &r)
	return r, err
}
func (w *WindowHandle) SetContentBounds(b Bounds) error {
	return w.Call("setContentBounds", []any{b}, nil)
}
func (w *WindowHandle) GetContentBounds() (Rect, error) {
	var r Rect
	err := w.Call("getContentBounds", nil, &r)
	return r, err
}
func (w *WindowHandle) GetNormalBounds() (Rect, error) {
	var r Rect
	err := w.Call("getNormalBounds", nil, &r)
	return r, err
}
func (w *WindowHandle) SetPosition(x, y int) error { return w.Call("setPosition", []any{x, y}, nil) }
func (w *WindowHandle) GetPosition() (int, int, error) {
	var p [2]int
	if err := w.Call("getPosition", nil, &p); err != nil {
		return 0, 0, err
	}
	return p[0], p[1], nil
}
func (w *WindowHandle) SetSize(width, height int) error {
	return w.Call("setSize", []any{width, height}, nil)
}
func (w *WindowHandle) GetSize() (int, int, error) {
	var p [2]int
	if err := w.Call("getSize", nil, &p); err != nil {
		return 0, 0, err
	}
	return p[0], p[1], nil
}
func (w *WindowHandle) SetContentSize(width, height int) error {
	return w.Call("setContentSize", []any{width, height}, nil)
}
func (w *WindowHandle) GetContentSize() (int, int, error) {
	var p [2]int
	if err := w.Call("getContentSize", nil, &p); err != nil {
		return 0, 0, err
	}
	return p[0], p[1], nil
}
func (w *WindowHandle) SetMinimumSize(width, height int) error {
	return w.Call("setMinimumSize", []any{width, height}, nil)
}
func (w *WindowHandle) GetMinimumSize() (int, int, error) {
	var p [2]int
	if err := w.Call("getMinimumSize", nil, &p); err != nil {
		return 0, 0, err
	}
	return p[0], p[1], nil
}
func (w *WindowHandle) SetMaximumSize(width, height int) error {
	return w.Call("setMaximumSize", []any{width, height}, nil)
}
func (w *WindowHandle) GetMaximumSize() (int, int, error) {
	var p [2]int
	if err := w.Call("getMaximumSize", nil, &p); err != nil {
		return 0, 0, err
	}
	return p[0], p[1], nil
}

// SetAspectRatio：extra 可为 nil；不为 nil 时追加为第二参数（窗口非内容区的额外尺寸）。
func (w *WindowHandle) SetAspectRatio(ratio float64, extra *Size) error {
	args := []any{ratio}
	if extra != nil {
		args = append(args, extra)
	}
	return w.Call("setAspectRatio", args, nil)
}

func (w *WindowHandle) Center() error { return w.Call("center", nil, nil) }

// 2. 状态切换（11）========================================================

func (w *WindowHandle) Minimize() error     { return w.Call("minimize", nil, nil) }
func (w *WindowHandle) Maximize() error     { return w.Call("maximize", nil, nil) }
func (w *WindowHandle) Unmaximize() error   { return w.Call("unmaximize", nil, nil) }
func (w *WindowHandle) Restore() error      { return w.Call("restore", nil, nil) }
func (w *WindowHandle) Hide() error         { return w.Call("hide", nil, nil) }
func (w *WindowHandle) Show() error         { return w.Call("show", nil, nil) }
func (w *WindowHandle) ShowInactive() error { return w.Call("showInactive", nil, nil) }
func (w *WindowHandle) Focus() error        { return w.Call("focus", nil, nil) }
func (w *WindowHandle) Blur() error         { return w.Call("blur", nil, nil) }
func (w *WindowHandle) SetFullScreen(flag bool) error {
	return w.Call("setFullScreen", []any{flag}, nil)
}

// 3. 状态查询（23，全部 bool）============================================

func boolCall(w *WindowHandle, method string) (bool, error) {
	var b bool
	err := w.Call(method, nil, &b)
	return b, err
}

func (w *WindowHandle) IsDestroyed() (bool, error)      { return boolCall(w, "isDestroyed") }
func (w *WindowHandle) IsVisible() (bool, error)        { return boolCall(w, "isVisible") }
func (w *WindowHandle) IsFocused() (bool, error)        { return boolCall(w, "isFocused") }
func (w *WindowHandle) IsMinimized() (bool, error)      { return boolCall(w, "isMinimized") }
func (w *WindowHandle) IsMaximized() (bool, error)      { return boolCall(w, "isMaximized") }
func (w *WindowHandle) IsFullScreen() (bool, error)     { return boolCall(w, "isFullScreen") }
func (w *WindowHandle) IsNormal() (bool, error)         { return boolCall(w, "isNormal") }
func (w *WindowHandle) IsModal() (bool, error)          { return boolCall(w, "isModal") }
func (w *WindowHandle) IsResizable() (bool, error)      { return boolCall(w, "isResizable") }
func (w *WindowHandle) IsMovable() (bool, error)        { return boolCall(w, "isMovable") }
func (w *WindowHandle) IsFocusable() (bool, error)      { return boolCall(w, "isFocusable") }
func (w *WindowHandle) IsMinimizable() (bool, error)    { return boolCall(w, "isMinimizable") }
func (w *WindowHandle) IsMaximizable() (bool, error)    { return boolCall(w, "isMaximizable") }
func (w *WindowHandle) IsClosable() (bool, error)       { return boolCall(w, "isClosable") }
func (w *WindowHandle) IsFullScreenable() (bool, error) { return boolCall(w, "isFullScreenable") }
func (w *WindowHandle) IsEnabled() (bool, error)        { return boolCall(w, "isEnabled") }
func (w *WindowHandle) HasShadow() (bool, error)        { return boolCall(w, "hasShadow") }
func (w *WindowHandle) IsAlwaysOnTop() (bool, error)    { return boolCall(w, "isAlwaysOnTop") }
func (w *WindowHandle) IsVisibleOnAllWorkspaces() (bool, error) {
	return boolCall(w, "isVisibleOnAllWorkspaces")
}
func (w *WindowHandle) IsKiosk() (bool, error)           { return boolCall(w, "isKiosk") }
func (w *WindowHandle) IsMenuBarVisible() (bool, error)  { return boolCall(w, "isMenuBarVisible") }
func (w *WindowHandle) IsMenuBarAutoHide() (bool, error) { return boolCall(w, "isMenuBarAutoHide") }
func (w *WindowHandle) IsDocumentEdited() (bool, error)  { return boolCall(w, "isDocumentEdited") }

// 4. 视觉属性（9）========================================================

func (w *WindowHandle) SetOpacity(opacity float64) error {
	return w.Call("setOpacity", []any{opacity}, nil)
}
func (w *WindowHandle) GetOpacity() (float64, error) {
	var v float64
	err := w.Call("getOpacity", nil, &v)
	return v, err
}
func (w *WindowHandle) SetBackgroundColor(color string) error {
	return w.Call("setBackgroundColor", []any{color}, nil)
}
func (w *WindowHandle) SetHasShadow(has bool) error {
	return w.Call("setHasShadow", []any{has}, nil)
}
func (w *WindowHandle) SetTitle(title string) error {
	return w.Call("setTitle", []any{title}, nil)
}
func (w *WindowHandle) GetTitle() (string, error) {
	var s string
	err := w.Call("getTitle", nil, &s)
	return s, err
}

// InvalidateShadow：macOS 透明窗口在内容大小变化后需手动失效阴影。
func (w *WindowHandle) InvalidateShadow() error { return w.Call("invalidateShadow", nil, nil) }
func (w *WindowHandle) FlashFrame(flag bool) error {
	return w.Call("flashFrame", []any{flag}, nil)
}

// SetProgressBar：progress ∈ [0,1] 显示进度；-1 关闭；>1 不确定。
func (w *WindowHandle) SetProgressBar(progress float64, opts *ProgressBarOptions) error {
	args := []any{progress}
	if opts != nil {
		args = append(args, opts)
	}
	return w.Call("setProgressBar", args, nil)
}

// 5. 层叠 / 鼠标 / 任务栏（7）============================================

// SetAlwaysOnTop：level 可空（等同于 Electron 不传第二个参数）。
// 可选值："normal" "floating" "torn-off-menu" "modal-panel"
// "main-menu" "status" "pop-up-menu" "screen-saver"。
func (w *WindowHandle) SetAlwaysOnTop(flag bool, level string) error {
	args := []any{flag}
	if level != "" {
		args = append(args, level)
	}
	return w.Call("setAlwaysOnTop", args, nil)
}
func (w *WindowHandle) SetIgnoreMouseEvents(ignore bool, opts *IgnoreMouseOptions) error {
	args := []any{ignore}
	if opts != nil {
		args = append(args, opts)
	}
	return w.Call("setIgnoreMouseEvents", args, nil)
}
func (w *WindowHandle) SetSkipTaskbar(skip bool) error {
	return w.Call("setSkipTaskbar", []any{skip}, nil)
}
func (w *WindowHandle) SetVisibleOnAllWorkspaces(visible bool, opts *VisibleOnAllWorkspacesOptions) error {
	args := []any{visible}
	if opts != nil {
		args = append(args, opts)
	}
	return w.Call("setVisibleOnAllWorkspaces", args, nil)
}
func (w *WindowHandle) MoveTop() error { return w.Call("moveTop", nil, nil) }
func (w *WindowHandle) MoveAbove(mediaSourceID string) error {
	return w.Call("moveAbove", []any{mediaSourceID}, nil)
}
func (w *WindowHandle) SetKiosk(flag bool) error { return w.Call("setKiosk", []any{flag}, nil) }

// 6. 能力开关（8）========================================================

func (w *WindowHandle) SetResizable(flag bool) error { return w.Call("setResizable", []any{flag}, nil) }
func (w *WindowHandle) SetMovable(flag bool) error   { return w.Call("setMovable", []any{flag}, nil) }
func (w *WindowHandle) SetFocusable(flag bool) error { return w.Call("setFocusable", []any{flag}, nil) }
func (w *WindowHandle) SetMinimizable(flag bool) error {
	return w.Call("setMinimizable", []any{flag}, nil)
}
func (w *WindowHandle) SetMaximizable(flag bool) error {
	return w.Call("setMaximizable", []any{flag}, nil)
}
func (w *WindowHandle) SetClosable(flag bool) error { return w.Call("setClosable", []any{flag}, nil) }
func (w *WindowHandle) SetFullScreenable(flag bool) error {
	return w.Call("setFullScreenable", []any{flag}, nil)
}
func (w *WindowHandle) SetEnabled(enabled bool) error {
	return w.Call("setEnabled", []any{enabled}, nil)
}

// 7. 菜单栏（3）==========================================================

func (w *WindowHandle) SetMenuBarVisibility(visible bool) error {
	return w.Call("setMenuBarVisibility", []any{visible}, nil)
}
func (w *WindowHandle) SetAutoHideMenuBar(hide bool) error {
	return w.Call("setAutoHideMenuBar", []any{hide}, nil)
}
func (w *WindowHandle) RemoveMenu() error { return w.Call("removeMenu", nil, nil) }

// 8. macOS 文档窗口（3）==================================================

func (w *WindowHandle) SetRepresentedFilename(path string) error {
	return w.Call("setRepresentedFilename", []any{path}, nil)
}
func (w *WindowHandle) GetRepresentedFilename() (string, error) {
	var s string
	err := w.Call("getRepresentedFilename", nil, &s)
	return s, err
}
func (w *WindowHandle) SetDocumentEdited(edited bool) error {
	return w.Call("setDocumentEdited", []any{edited}, nil)
}

// 9. 内容加载（3）========================================================

func (w *WindowHandle) LoadURL(url string, options map[string]any) error {
	args := []any{url}
	if options != nil {
		args = append(args, options)
	}
	return w.Call("loadURL", args, nil)
}
func (w *WindowHandle) LoadFile(path string, options map[string]any) error {
	args := []any{path}
	if options != nil {
		args = append(args, options)
	}
	return w.Call("loadFile", args, nil)
}
func (w *WindowHandle) Reload() error { return w.Call("reload", nil, nil) }

// 10. WebContents（22）===================================================

// WebContents 返回 webContents 子对象门面，等价于 Electron 原生写法
// `win.webContents.send(...)` / `win.webContents.openDevTools()`。
func (w *WindowHandle) WebContents() *WebContents { return &WebContents{w: w} }

// WebContents 是 WindowHandle.WebContents() 返回的子对象，封装 webContents.* 方法。
type WebContents struct{ w *WindowHandle }

func (wc *WebContents) Send(channel string, args ...any) error {
	return wc.w.callWithParent("webContents.send", append([]any{channel}, args...), "", nil)
}
func (wc *WebContents) ExecuteJavaScript(code string, userGesture *bool) (json.RawMessage, error) {
	args := []any{code}
	if userGesture != nil {
		args = append(args, *userGesture)
	}
	var raw json.RawMessage
	err := wc.w.Call("webContents.executeJavaScript", args, &raw)
	return raw, err
}
func (wc *WebContents) OpenDevTools(opts *OpenDevToolsOptions) error {
	args := []any{}
	if opts != nil {
		args = append(args, opts)
	}
	return wc.w.Call("webContents.openDevTools", args, nil)
}
func (wc *WebContents) CloseDevTools() error { return wc.w.Call("webContents.closeDevTools", nil, nil) }
func (wc *WebContents) ToggleDevTools() error {
	return wc.w.Call("webContents.toggleDevTools", nil, nil)
}
func (wc *WebContents) IsDevToolsOpened() (bool, error) {
	var b bool
	err := wc.w.Call("webContents.isDevToolsOpened", nil, &b)
	return b, err
}
func (wc *WebContents) GoBack() error    { return wc.w.Call("webContents.goBack", nil, nil) }
func (wc *WebContents) GoForward() error { return wc.w.Call("webContents.goForward", nil, nil) }
func (wc *WebContents) CanGoBack() (bool, error) {
	var b bool
	err := wc.w.Call("webContents.canGoBack", nil, &b)
	return b, err
}
func (wc *WebContents) CanGoForward() (bool, error) {
	var b bool
	err := wc.w.Call("webContents.canGoForward", nil, &b)
	return b, err
}
func (wc *WebContents) GetURL() (string, error) {
	var s string
	err := wc.w.Call("webContents.getURL", nil, &s)
	return s, err
}
func (wc *WebContents) GetTitle() (string, error) {
	var s string
	err := wc.w.Call("webContents.getTitle", nil, &s)
	return s, err
}
func (wc *WebContents) SetZoomFactor(f float64) error {
	return wc.w.Call("webContents.setZoomFactor", []any{f}, nil)
}
func (wc *WebContents) GetZoomFactor() (float64, error) {
	var v float64
	err := wc.w.Call("webContents.getZoomFactor", nil, &v)
	return v, err
}
func (wc *WebContents) SetZoomLevel(l float64) error {
	return wc.w.Call("webContents.setZoomLevel", []any{l}, nil)
}
func (wc *WebContents) GetZoomLevel() (float64, error) {
	var v float64
	err := wc.w.Call("webContents.getZoomLevel", nil, &v)
	return v, err
}
func (wc *WebContents) Copy() error      { return wc.w.Call("webContents.copy", nil, nil) }
func (wc *WebContents) Paste() error     { return wc.w.Call("webContents.paste", nil, nil) }
func (wc *WebContents) Cut() error       { return wc.w.Call("webContents.cut", nil, nil) }
func (wc *WebContents) SelectAll() error { return wc.w.Call("webContents.selectAll", nil, nil) }
func (wc *WebContents) Undo() error      { return wc.w.Call("webContents.undo", nil, nil) }
func (wc *WebContents) Redo() error      { return wc.w.Call("webContents.redo", nil, nil) }

// ScopedWindowHandle 是 command 作用域下的窗口句柄。普通窗口方法继续不带
// parentRequestId，只有 WebContents().Send 会携带当前 command parent。
type ScopedWindowHandle struct {
	*WindowHandle
	parentRequestID string
	trace           *TraceContext
}

func (w *ScopedWindowHandle) WebContents() *ScopedWebContents {
	return &ScopedWebContents{WebContents: w.WindowHandle.WebContents(), w: w}
}

type ScopedWebContents struct {
	*WebContents
	w *ScopedWindowHandle
}

func (wc *ScopedWebContents) Send(channel string, args ...any) error {
	return wc.w.WindowHandle.callWithParentTrace("webContents.send", append([]any{channel}, args...), wc.w.parentRequestID, wc.w.trace, nil)
}

func explicitPayloadRequestID(args []any) string {
	if len(args) < 2 {
		return ""
	}
	payload, ok := args[1].(map[string]any)
	if !ok {
		return ""
	}
	requestID, _ := payload["requestId"].(string)
	return requestID
}
