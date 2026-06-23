package brickly

// WindowOptions 对应 host.ui.createBrowserWindow 的 options 字段。
// 用 map 以保持与 schema 自由演进同步；常用键请参考 specs/window-api.md。
type WindowOptions map[string]any

// UI 是 Brick 级 UI 门面，提供子窗口相关能力。
// 通过 Runtime.UI 或 CommandContext.UI() 获取。
type UI struct {
	runtime *Runtime
}

// CreateBrowserWindow 创建子窗口并返回句柄。
// url 可以是 http(s)、file:// 或相对 ui/ 目录的 html 路径。
func (u *UI) CreateBrowserWindow(url string, options WindowOptions) (*WindowHandle, error) {
	msg := map[string]any{
		"type": "host.ui.createBrowserWindow",
		"url":  url,
	}
	if options != nil {
		msg["options"] = options
	}
	var res struct {
		WindowID int64 `json:"windowId"`
	}
	if err := u.runtime.transport.hostCall(msg, &res); err != nil {
		return nil, err
	}
	h := newWindowHandle(u.runtime, res.WindowID)
	u.runtime.windowsMu.Lock()
	u.runtime.windows[res.WindowID] = h
	u.runtime.windowsMu.Unlock()
	return h, nil
}

// ListWindows 列出本 Brick 持有的窗口描述（结构由宿主定义）。
func (u *UI) ListWindows() ([]map[string]any, error) {
	var out []map[string]any
	err := u.runtime.transport.hostCall(map[string]any{
		"type": "host.ui.listWindows",
	}, &out)
	return out, err
}

// ScopedUI 是 command 作用域下的 UI 门面。它创建的窗口句柄会在
// webContents.Send 时携带当前 command 的 parentRequestId。
type ScopedUI struct {
	runtime         *Runtime
	parentRequestID string
}

func (u *ScopedUI) CreateBrowserWindow(url string, options WindowOptions) (*ScopedWindowHandle, error) {
	win, err := (&UI{runtime: u.runtime}).CreateBrowserWindow(url, options)
	if err != nil {
		return nil, err
	}
	return &ScopedWindowHandle{WindowHandle: win, parentRequestID: u.parentRequestID}, nil
}

func (u *ScopedUI) ListWindows() ([]map[string]any, error) {
	return (&UI{runtime: u.runtime}).ListWindows()
}
