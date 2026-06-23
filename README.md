# brickly-sdk-go

Brickly Brick **Go runtime** 官方 SDK。是 [`@syllm/brickly-sdk`](../brickly-sdk-node) (Node) 的 Go 对照实现，API 语义与协议版本完全一致。

零外部依赖，只用 Go 标准库（`bufio` / `encoding/json` / `io` / `sync` / `context`）。

---

## 快速上手

```go
package main

import (
    "encoding/json"

    brickly "github.com/836145715/brickly-sdk-go"
)

func main() {
    p := brickly.New(brickly.Options{BrickID: "com.example.go-demo"})

    p.OnCommand("hello", func(ctx *brickly.CommandContext, input json.RawMessage) (any, error) {
        ctx.Progress(0.5, "thinking...")
        return map[string]any{"ok": true, "echo": string(input)}, nil
    })

    p.OnReady(func() error {
        win, err := p.UI.CreateBrowserWindow("ui/pet.html", brickly.WindowOptions{
            "width": 200, "height": 200,
            "frame": false, "transparent": true, "alwaysOnTop": true,
        })
        if err != nil {
            return err
        }
        win.On("closed", func(_ map[string]any) {
            _ = p.Events.Publish("pet.closed", map[string]any{"id": win.ID})
        })
        return nil
    })

    p.Start() // 阻塞直到 runtime.shutdown 或 stdin 关闭
}
```

SDK 自动完成：

- `host.hello` → `runtime.ready` 握手
- `runtime.ping` → `runtime.pong` 心跳
- `host.*` 请求 id 分配与 `host.result` / `host.error` 路由
- `command.invoke` 分发与 `command.result` / `command.error` 序列化
- `command.cancel` → `context.Context` 取消信号
- `runtime.shutdown` → `OnShutdown` 钩子 → `runtime.bye` → 退出

---

## 核心 API

### `Runtime`（`brickly.New(Options{...})` 返回）

| 方法 | 作用 |
|---|---|
| `OnCommand(id, handler)` | 注册命令处理器（链式） |
| `OnReady(fn)` | runtime.ready 之后异步触发 |
| `OnShutdown(fn)` | runtime.shutdown 时触发，返回后 SDK 自动发 runtime.bye 退出 |
| `UI.CreateBrowserWindow(url, opts)` | 创建子窗口，返回 `*WindowHandle` |
| `UI.ListWindows()` | 列出本 Brick 持有的窗口 |
| `Events.On(event, fn)` | 订阅事件总线（含 `window.*` 系列），返回取消函数 |
| `Events.Publish(event, payload)` | 发布事件 |
| `Platform.System.*` / `System.*` | 调用宿主系统能力（System 是便捷别名） |
| `Platform.Clipboard.*` | 读取或写入系统剪贴板 |
| `Invoke(brickID, commandID, input, into, opts...)` | command 作用域内的 child 调用底层入口；普通业务使用 `ctx.Invoke` |
| `InvokeRoot(brickID, commandID, input, into, opts...)` | command 外发起 root 跨 Brick 调用 |
| `InvokeStream(brickID, commandID, input, opts...)` | command 作用域内的流式 child 调用底层入口；普通业务使用 `ctx.InvokeStream` |
| `OpenSession(brickID, opts...)` | 打开跨 Brick 会话；普通业务使用 `ctx.OpenSession` |
| `Start()` | 启动 stdin 循环（阻塞） |
| `Logf(format, args...)` | 写 stderr 日志；宿主日志中心自动关联 Brick 身份 |

### `CommandContext`（handler 第一个参数）

| 字段 / 方法 | 作用 |
|---|---|
| `RequestID` / `CommandID` | 当前请求与命令 id |
| `Progress(value, message)` | 进度（value ∈ [0,1]） |
| `Chunk(name, chunk)` | 向具名输出追加流式片段 |
| `Output(name, value)` | 一次性覆盖具名输出 |
| `Context()` | `context.Context`，收到 `command.cancel` 时被取消 |
| `IsCancelled()` | 协作式取消轮询 |
| `Invoke(brickID, commandID, input, into, opts...)` | 跨 Brick 调用命令，自动携带当前 `parentRequestId`，支持 `WithProfileID` |
| `InvokeStream(brickID, commandID, input, opts...)` | 跨 Brick 流式调用命令，自动携带当前 `parentRequestId`，支持 `WithProfileID` |
| `OpenSession(brickID, opts...)` | 打开跨 Brick 会话，支持 `WithSessionProfileID`；`session.Invoke` 自动携带当前 `parentRequestId` |
| `UI()` / `Events()` | 与 `Runtime.UI` / `Runtime.Events` 同源 |
| `Platform()` / `System()` | 与 `Runtime.Platform` / `Runtime.System` 同源 |

### `CommandHandler` 签名

```go
type CommandHandler func(ctx *CommandContext, input json.RawMessage) (any, error)
```

返回 `*BppError` 会保留 `code` 字段；其他 `error` 会被映射为 `INTERNAL_ERROR`。

---

## 日志约定

`Logf(format, args...)` 只写入 stderr，stdout 永远只写 BPP 协议消息。SDK 不会把 `BrickID` 拼进日志正文；宿主日志中心会在采集 stderr 时用结构化字段记录来源、Brick id、stream 与作用域名称。Brick 代码不要手动输出 `[brickId]` 前缀，避免日志中心和 SQLite 中出现重复信息。

---

## 跨 Brick 调用

普通 Brick 在 command handler 内调用其它 Brick 命令时只需要 `ctx.Invoke`。SDK 会自动携带当前请求的 `parentRequestId`，宿主会把子调用挂到同一 invocation graph 下，并自动启动、复用和回收目标 Brick 实例。目标 Brick 需要配置时，用 `WithProfileID` 指定目标 Brick Profile；不传则使用目标 Brick 默认 Profile。

```go
var out map[string]any
err := ctx.Invoke(
    "com.brickly.openai",
    "chat",
    map[string]any{"prompt": "hello"},
    &out,
    brickly.WithProfileID("work"),
)
```

调用方 manifest 必须在 `dependencies` 中声明目标 Brick 和允许调用的命令：

```json
"dependencies": {
  "com.brickly.openai": {
    "commands": ["chat"]
  }
}
```

如果需要在 command 外主动创建顶级调用，使用显式 root API：

```go
var out map[string]any
err := p.InvokeRoot(
    "com.brickly.openai",
    "chat",
    map[string]any{"prompt": "hello"},
    &out,
    brickly.WithProfileID("work"),
)
```

### 流式跨 Brick 调用

目标命令会产生 `progress`、`chunk`、`output` 等流式事件时，使用 `InvokeStream`。SDK 会发送 `host.invoke` 且带上 `stream: true`，然后按宿主返回顺序产出事件，最终 `host.result` 会变成 `Type == "result"` 的事件。

```go
events, errs := ctx.InvokeStream(
    "com.brickly.openai",
    "chat-completions",
    map[string]any{"stream": true, "messages": messages},
    brickly.WithProfileID("work"),
)

for event := range events {
    switch event.Type {
    case "chunk":
        if event.Name == "text" {
            fmt.Print(event.Chunk)
        }
    case "result":
        var final map[string]any
        _ = json.Unmarshal(event.Result, &final)
    }
}
if err := <-errs; err != nil {
    return nil, err
}
```

## 跨 Brick 会话

当目标 Brick 实例内部有状态，需要在多次命令调用之间保留上下文时，在 command handler 内使用 `ctx.OpenSession`。同一个 session 的后续 `Invoke` 会落到同一个目标 Brick 实例；每次 `session.Invoke` 都会携带创建它且仍在执行的 command `parentRequestId`。调用 `Close()`、调用方 Brick 实例退出或宿主回收调用方时，会话会结束。

```go
session, err := ctx.OpenSession("com.brickly.openai", brickly.WithSessionProfileID("work"))
if err != nil {
    return nil, err
}
defer session.Close()

if err := session.Invoke("start-thread", map[string]any{"title": "Draft"}, nil); err != nil {
    return nil, err
}

var reply map[string]any
if err := session.Invoke("chat", map[string]any{"prompt": "继续刚才的话题"}, &reply); err != nil {
    return nil, err
}
return reply, nil
```

`WithSessionProfileID` 指定的是目标 Brick Profile ID。后续 `session.Invoke` 每次都会按调用方 manifest 的 `dependencies[target].commands` 重新校验命令。

---

## 平台 System API

`p.Platform.System.*`、`p.System.*` 与 handler 内的 `ctx.Platform().System.*`、`ctx.System().*` 通过 BPP `host.platform.system.*` 调用宿主系统能力：

```go
p.OnCommand("show-app-info", func(ctx *brickly.CommandContext, _ json.RawMessage) (any, error) {
	appName, err := ctx.System().GetAppName()
	if err != nil {
		return nil, err
	}
	appVersion, err := ctx.System().GetAppVersion()
	if err != nil {
		return nil, err
	}
	userData, err := ctx.System().GetPath(brickly.SystemPathUserData)
	if err != nil {
		return nil, err
	}
	isMacOS, err := ctx.System().IsMacOS()
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"appName": appName,
		"appVersion": appVersion,
		"userData": userData,
		"isMacOS": isMacOS,
	}, nil
})
```

当前方法包括 `ShowNotification`、`ShellOpenPath`、`ShellTrashItem`、`ShellShowItemInFolder`、`ShellOpenExternal`、`ShellBeep`、`GetNativeID`、`GetAppName`、`GetAppVersion`、`GetPath`、`GetFileIcon`、`ReadCurrentFolderPath`、`ReadCurrentBrowserURL`、`IsDev`、`IsMacOS`、`IsWindows`、`IsLinux`。

`GetPath()` 支持 `SystemPathHome`、`SystemPathAppData`、`SystemPathAssets`、`SystemPathUserData`、`SystemPathSessionData`、`SystemPathTemp`、`SystemPathExe`、`SystemPathModule`、`SystemPathDesktop`、`SystemPathDocuments`、`SystemPathDownloads`、`SystemPathMusic`、`SystemPathPictures`、`SystemPathVideos`、`SystemPathRecent`、`SystemPathLogs`、`SystemPathCrashDumps`。runtime 侧仍按 manifest 权限校验：通知需要 `os.notification`；Shell 类能力需要 `os.exec`；应用信息、路径、设备 ID、平台判断和当前文件夹路径读取需要 `os.env`；文件图标需要 `fs.read`。`ReadCurrentFolderPath()` 在 macOS Finder 与 Windows Explorer 前台窗口可用；当前没有可读取的前台文件管理器文件夹时会返回 `CURRENT_FOLDER_UNAVAILABLE`。`ReadCurrentBrowserURL()` 当前预留，会返回 `UNSUPPORTED_PLATFORM`。

## 平台 Clipboard API

`p.Platform.Clipboard.*` 与 handler 内的 `ctx.Platform().Clipboard.*` 通过 BPP `host.platform.clipboard.*` 调用宿主剪贴板能力。runtime 侧仍按 manifest 权限校验，调用方需要声明 `os.clipboard`。

```go
snapshot, err := ctx.Platform().Clipboard.ReadContent()
if err != nil {
    return nil, err
}

_, err = ctx.Platform().Clipboard.SetContent(brickly.ClipboardContent{
    "kind": "text",
    "text": "hello",
})
if err != nil {
    return nil, err
}

return snapshot, nil
```

---

## WindowHandle（107 个方法）

`UI.CreateBrowserWindow` 返回的 `*WindowHandle` 完整封装了宿主白名单全部 107 个方法。详细签名/参数/返回值见 [`specs/window-api.md`](../../../specs/window-api.md)。

约定：

- 所有 `SetXxx` / 动作方法 → 返回 `error`
- 所有 `GetXxx` / `IsXxx` → 返回 `(值, error)`
- 关闭后再调用（除 `IsDestroyed`）立即返回 `INVALID_INPUT`
- 不在 SDK 表面的新方法可用 `win.Call(method, args, into)` 兜底

### 1. 几何 / 位置（17）

```go
win.SetBounds(brickly.Bounds{X: ptr(100), Y: ptr(50)})  // 全部字段可选
r, _ := win.GetBounds()                                 //=> Rect{X,Y,Width,Height}
win.SetContentBounds(brickly.Bounds{...})  win.GetContentBounds()
win.GetNormalBounds()                                    // 非最大化/最小化时的"常态"
win.SetPosition(100, 50)        win.GetPosition()        //=> (x, y, err)
win.SetSize(640, 480)           win.GetSize()
win.SetContentSize(...)         win.GetContentSize()
win.SetMinimumSize(w, h)        win.GetMinimumSize()
win.SetMaximumSize(w, h)        win.GetMaximumSize()
win.SetAspectRatio(16.0/9.0, nil)
win.SetAspectRatio(16.0/9.0, &brickly.Size{Width: 40, Height: 50})
win.Center()
```

### 2. 状态切换（12）

```go
win.Minimize()  win.Maximize()  win.Unmaximize()  win.Restore()
win.Hide()      win.Show()      win.ShowInactive()
win.Focus()     win.Blur()
win.SetFullScreen(true)
win.Destroy()   // 强制销毁，不触发 close 事件
win.Close()     // 走 host.ui.closeWindow，触发 window.closed
```

### 3. 状态查询（23，全部 `(bool, error)`）

```go
win.IsDestroyed()  win.IsVisible()  win.IsFocused()
win.IsMinimized()  win.IsMaximized()  win.IsFullScreen()
win.IsNormal()  win.IsModal()
win.IsResizable()  win.IsMovable()  win.IsFocusable()
win.IsMinimizable()  win.IsMaximizable()  win.IsClosable()  win.IsFullScreenable()
win.IsEnabled()  win.HasShadow()
win.IsAlwaysOnTop()  win.IsVisibleOnAllWorkspaces()  win.IsKiosk()
win.IsMenuBarVisible()  win.IsMenuBarAutoHide()  win.IsDocumentEdited()
```

### 4. 视觉属性（9）

```go
win.SetOpacity(0.8)        win.GetOpacity()
win.SetBackgroundColor("#00000000")
win.SetHasShadow(true)
win.SetTitle("Hello")      win.GetTitle()
win.InvalidateShadow()                  // macOS 透明窗口阴影刷新
win.FlashFrame(true)
win.SetProgressBar(0.42, &brickly.ProgressBarOptions{Mode: "normal"})
```

### 5. 层叠 / 鼠标 / 任务栏（7）

```go
win.SetAlwaysOnTop(true, "")              // level 留空 = 不传第二参数
win.SetAlwaysOnTop(true, "screen-saver")
win.SetIgnoreMouseEvents(true, &brickly.IgnoreMouseOptions{Forward: true})
win.SetSkipTaskbar(true)
win.SetVisibleOnAllWorkspaces(true, &brickly.VisibleOnAllWorkspacesOptions{VisibleOnFullScreen: true})
win.MoveTop()
win.MoveAbove("window:42:0")              // mediaSourceId
win.SetKiosk(true)
```

### 6. 能力开关（8）

```go
win.SetResizable(false)       win.SetMovable(false)
win.SetFocusable(false)       win.SetMinimizable(false)
win.SetMaximizable(false)     win.SetClosable(false)
win.SetFullScreenable(false)  win.SetEnabled(false)
```

### 7. 菜单栏（3）

```go
win.SetMenuBarVisibility(false)
win.SetAutoHideMenuBar(true)
win.RemoveMenu()
```

### 8. macOS 文档窗口（3）

```go
win.SetRepresentedFilename("/path/to/file.txt")
win.GetRepresentedFilename()
win.SetDocumentEdited(true)
```

### 9. 内容加载（3）

```go
win.LoadURL("https://example.com", nil)
win.LoadFile("ui/pet.html", nil)
win.Reload()
```

### 10. WebContents（22）

```go
wc := win.WebContents()
wc.Send("channel", "arg1", map[string]any{"x": 1})
raw, _ := wc.ExecuteJavaScript("document.title", nil)  // raw 为 json.RawMessage
wc.OpenDevTools(&brickly.OpenDevToolsOptions{Mode: "detach"})
wc.CloseDevTools()  wc.ToggleDevTools()  wc.IsDevToolsOpened()

wc.GoBack()  wc.GoForward()  wc.CanGoBack()  wc.CanGoForward()
wc.GetURL()  wc.GetTitle()

wc.SetZoomFactor(1.25)  wc.GetZoomFactor()
wc.SetZoomLevel(1)      wc.GetZoomLevel()

wc.Copy()  wc.Paste()  wc.Cut()  wc.SelectAll()  wc.Undo()  wc.Redo()
```

### 通用兜底：`win.Call`

```go
// 当宿主开放了新方法、但 SDK 还没升级时，可直接走 host.ui.callWindow：
var url string
err := win.Call("webContents.getURL", nil, &url)
```

### 事件订阅

```go
unsub := win.On("closed", func(payload map[string]any) {
    fmt.Println("window closed:", payload["windowId"])
})
defer unsub()
// 其他事件：focus / blur / message / resize / move / minimize / maximize /
// unmaximize / restore / show / hide / enter-full-screen / leave-full-screen
```

---

## 取消语义

```go
p.OnCommand("long-task", func(ctx *brickly.CommandContext, _ json.RawMessage) (any, error) {
    for i := 0; i < 100; i++ {
        select {
        case <-ctx.Context().Done():
            return nil, brickly.NewBppError("CANCELLED", "user cancelled")
        default:
        }
        ctx.Progress(float64(i+1)/100, "")
        time.Sleep(50 * time.Millisecond)
    }
    return map[string]any{"done": true}, nil
})
```

---

## 错误返回

```go
return nil, brickly.NewBppError("INVALID_INPUT", "text is required")
```

`BppError.Code` 取值以 `specs/bpp.schema.json` 的 `BridgeErrorCode` 为准，常见值包括：
`INVALID_INPUT` / `BRICK_NOT_FOUND` / `COMMAND_NOT_FOUND` / `DEPENDENCY_NOT_DECLARED` /
`DEPENDENCY_COMMAND_NOT_ALLOWED` / `PERMISSION_DENIED` / `CURRENT_FOLDER_UNAVAILABLE` /
`CANCELLED` / `INTERNAL_ERROR`。

普通 `error` 会被映射成 `INTERNAL_ERROR`。

---

## 协议与版本对齐

- **白名单真相源**：[`specs/bpp.schema.json`](../../../specs/bpp.schema.json) 的 `BrickWindowMethod.enum`
- **跨语言协议规范**：[`specs/window-api.md`](../../../specs/window-api.md)（Node / Go / Python SDK 共用）
- 当前协议版本：`0.1.0`，窗口 API 子集版本 `v0.2`（107 个方法）
- `protocol.go` 的 `BrickWindowMethods` 由 `TestWhitelistMatchesSchema` 强制与 schema 完全同步

---

## 测试

```bash
go test ./...
go test -v -run TestWhitelistMatchesSchema ./...   # 校验白名单与 schema 同步
```

测试在同一进程内用 `io.Pipe` 模拟宿主-runtime 双向通信，覆盖：

- `host.hello` → `runtime.ready` 握手与 `OnReady` 钩子
- `runtime.ping` → `runtime.pong` 心跳
- `command.invoke` 完整生命周期（progress / output / result）
- `command.error` 与 `COMMAND_NOT_FOUND`
- `hostCall` 请求-响应配对（创建窗口）
- `WindowHandle.SetTitle` 反射到 `host.ui.callWindow`
- 白名单与 `specs/bpp.schema.json` 严格相等

---

## 在 Brick 中使用

由于本仓库 monorepo 布局，在 Brick 的 `runtime/go/go.mod` 中加 `replace` 指向 SDK 源码即可：

```
module com.example.mybrick

go 1.21

require github.com/836145715/brickly-sdk-go v0.0.0-00010101000000-000000000000

replace github.com/836145715/brickly-sdk-go => ../../../../packages/brickly-sdk-go
```

需要分发独立二进制时 `go build` 会把 SDK 静态链接进可执行文件，零运行时依赖。

---

## 与 Node SDK 的对照

| Node SDK | Go SDK |
|---|---|
| `new BricklyRuntime({ brickId })` | `brickly.New(brickly.Options{BrickID: ...})` |
| `brick.onCommand(id, fn)` | `p.OnCommand(id, fn)` |
| `ctx.progress(v, msg)` | `ctx.Progress(v, msg)` |
| `ctx.ui.createBrowserWindow(url, opts)` | `ctx.UI().CreateBrowserWindow(url, opts)` |
| `win.setBounds({...})` | `win.SetBounds(brickly.Bounds{...})` |
| `Promise<T>` | `(T, error)` |
| `events.on(event, fn)` 返回 unsubscribe | `Events.On(event, fn)` 返回 unsubscribe |
| `new BppError('CODE', msg)` | `brickly.NewBppError("CODE", msg)` |
| `win.webContents.send(...)` | `win.WebContents().Send(...)` |

行为一致：序列化字节级等价、协议请求 ID 前缀 `<brickId>-<pid>-<seq>`、stdout 仅写协议、stderr 写纯日志正文。
