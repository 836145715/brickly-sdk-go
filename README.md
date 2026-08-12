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

| 方法                                                   | 作用                                                                       |
| ------------------------------------------------------ | -------------------------------------------------------------------------- |
| `OnCommand(id, handler)`                               | 注册命令处理器（链式）                                                     |
| `OnReady(fn)`                                          | runtime.ready 之后异步触发                                                 |
| `OnShutdown(fn)`                                       | runtime.shutdown 时触发，返回后 SDK 自动发 runtime.bye 退出                |
| `UI.CreateBrowserWindow(url, opts)`                    | 创建子窗口，返回 `*WindowHandle`                                           |
| `UI.ListWindows()`                                     | 列出本 Brick 持有的窗口                                                    |
| `Events.On(event, fn)`                                 | 订阅事件总线（含 `window.*` 系列），返回取消函数                           |
| `Events.Publish(event, payload)`                       | 发布事件                                                                   |
| `Platform.System.*` / `System.*`                       | 调用宿主系统能力（System 是便捷别名）                                      |
| `Platform.Clipboard.*`                                 | 读取或写入系统剪贴板                                                       |
| `Invoke(brickID, commandID, input, into, opts...)`     | command 作用域内的 child 调用底层入口；普通业务使用 `ctx.Invoke`           |
| `InvokeRoot(brickID, commandID, input, into, opts...)` | command 外发起 root 跨 Brick 调用                                          |
| `InvokeStream(brickID, commandID, input, opts...)`     | command 作用域内的流式 child 调用底层入口；普通业务使用 `ctx.InvokeStream` |
| `OpenSession(brickID, opts...)`                        | 打开跨 Brick 会话；普通业务使用 `ctx.OpenSession`                          |
| `OpenResource(ref)`                                    | 惰性绑定已有 `ResourceRef`，不立即访问 Host                                |
| `Start()`                                              | 启动 stdin 循环（阻塞）                                                    |
| `Debug/Info/Warn/Error(message, fields)`               | 结构化日志 → `runtime.log`（带 level，推荐）                               |

### `CommandContext`（handler 第一个参数）

| 字段 / 方法                                        | 作用                                                                                                                     |
| -------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------ |
| `RequestID` / `CommandID`                          | 当前请求与命令 id                                                                                                        |
| `Invocation`                                      | 宿主注入的可信调用来源；未提供时 `Source` 为 `unknown`                                                                   |
| `Progress(value, message)`                         | 进度（value ∈ [0,1]）                                                                                                    |
| `Chunk(name, chunk) error`                         | 向具名输出追加流式片段；资源引用无效或 payload 过深时返回错误                                                            |
| `Output(name, value) error`                        | 一次性覆盖具名输出；资源引用无效或 payload 过深时返回错误                                                                |
| `Context()`                                        | `context.Context`，收到 `command.cancel` 时被取消                                                                        |
| `IsCancelled()`                                    | 协作式取消轮询                                                                                                           |
| `CreateResource(content, options)`                 | 在当前 command 生命周期内创建资源；大内容自动绑定上传归属                                                               |
| `CreateResourceFrom(reader, options)`              | 从 `io.Reader` 流式创建绑定当前 command 生命周期的资源                                                                  |
| `CreateResourceWriter(options)`                    | 创建绑定当前 command 生命周期的 store-and-forward writer                                                               |
| `Invoke(brickID, commandID, input, into, opts...)` | 跨 Brick 调用命令，自动携带当前 `parentRequestId`，支持 `WithProfileID`                                                  |
| `InvokeResource(brickID, commandID, input, opts...)` | 跨 Brick 调用并始终返回 `*ResourceHandle`                                                               |
| `InvokeRoot(brickID, commandID, input, into, opts...)` | 发起独立 root 调用，携带当前 `parentRequestId` 供宿主审计                                                            |
| `InvokeRootResource(brickID, commandID, input, opts...)` | Root 调用并始终返回 `*ResourceHandle`                                                               |
| `InvokeStream(brickID, commandID, input, opts...)` | 跨 Brick 流式调用命令，自动携带当前 `parentRequestId`，支持 `WithProfileID`                                              |
| `OpenSession(brickID, opts...)`                    | 打开跨 Brick 会话，支持 `WithSessionProfileID`；`session.Invoke` / `session.InvokeStream` 自动携带当前 `parentRequestId` |
| `UI()` / `Events()`                                | 与 `Runtime.UI` / `Runtime.Events` 同源                                                                                  |
| `Platform()` / `System()`                          | 与 `Runtime.Platform` / `Runtime.System` 同源                                                                            |

`Invocation.DependencyProfiles` 为目标 Brick 指定默认 Profile。`Invoke`、`InvokeRoot`、`InvokeStream` 和 `OpenSession` 在未显式传 Profile 时自动使用该映射；`WithProfileID` / `WithSessionProfileID` 始终优先。

资源调用返回 `*ResourceHandle`，实现 `io.ReadCloser`，按块读取宿主资源并提供 `Text()`、`JSON(out)`、`SaveTo(path)`、`Revoke()`。资源超过 200 MiB 时不能整体物化，应使用流读取或直接保存到文件；将句柄再次作为输入时 SDK 只传递 `ResourceRef`。

Go command handler 接收 `json.RawMessage`。输入包含资源时，先把对应字段解码为 `ResourceRef`，
再调用 `runtime.OpenResource(ref)` 获得绑定当前 transport 的可读句柄。Node、Python 和 Renderer
同样保留业务 JSON 中的 Ref，并通过各自的 `resources.open(ref)` 显式打开。

Brick 可通过 `runtime.CreateResource(content, options)` 主动创建资源；`content` 只接受
`string` 或 `[]byte`。字符串默认 `text/plain; charset=utf-8`，字节默认
`application/octet-stream`，通常可传 `nil` options。该能力无需声明 manifest 权限，但仍受 Host
配额与生命周期治理。小内容走
一次性快速路径，大内容自动切换到 Writer，调用方式和返回类型不变。

大内容使用 `runtime.CreateResourceFrom(reader, options)`。它从 `io.Reader` 自动聚合并按最大 1 MiB
顺序写入 Host，finish 后返回 `*ResourceHandle`；通常可直接传 `nil` options。finish 前资源
不可读取，发布后下游读取速度不会影响已经结束的上传。资源总大小不受普通 invoke 的 200 MiB
上限约束。Host 限制并发上传并在生产环境保留 1 GiB 磁盘安全余量；部署还可配置全局和
Brick 维度的 pending bytes 配额。

需要主动分多次写入时使用 `CreateResourceWriter`。Writer 实现 `io.Writer` 和 `io.ReaderFrom`：

```go
writer, err := runtime.CreateResourceWriter(options)
_, err = writer.Write(header)
_, err = writer.ReadFrom(downloadStream)
resource, err := writer.Finish()
```

同一 Writer 的 `Write`、`ReadFrom`、`Finish`、`Abort` 按调用顺序串行执行；`Abort` 不会越过
已经开始的数据源操作。`WriteString` 直接分段复制到内部缓冲，不会额外创建完整 `[]byte` 副本。

普通 `Invoke` / `InvokeRoot` 始终解码为直接值，逻辑 JSON 输入和结果上限为 200 MiB；
超限返回 `PAYLOAD_TOO_LARGE`，不会静默改成资源类型。资源结果应从第一次调用起使用
`InvokeResource`、`InvokeRootResource` 或 `session.InvokeResource`。EventBus 回调统一收到
外层 `*ResourceHandle`，先调用 `JSON(&payload)` 取得业务对象。资源内容按普通 JSON
解析，内嵌 `ResourceRef` 不会自动水合，需要读取时应显式转换。Capability token 不得写入
日志或持久化，Ref 只能在同一宿主和 TTL 内使用。无论事件大小，回调都不会收到内联对象或
内部 `{"resource": ..., "encoding": "json"}` 包装；消费完成后应调用 `Close()`。

SDK 在发送 invoke、stream、command 结果、chunk、output 或事件时，会自动把嵌套
`*ResourceHandle` 转成完整 `ResourceRef`。`OpenResource` 只做校验和 transport 绑定，不会立即访问
Host：

```go
handle, err := runtime.OpenResource(payload.Attachment)
if err != nil {
    return nil, err
}
defer handle.Close()
```

### `CommandHandler` 签名

```go
type CommandHandler func(ctx *CommandContext, input json.RawMessage) (any, error)
```

返回 `*BppError` 会保留 `code` 字段；其他 `error` 会被映射为 `INTERNAL_ERROR`。

---

## 日志约定

业务日志**必须**使用带 level 的 API：

```go
p.Info("ready", nil)
p.Warn("retry", map[string]any{"n": 2})
p.Error("failed", err, nil)
// 命令内（自动挂当前 Trace）:
ctx.Info("search started", map[string]any{"pattern": q})
```

对应 BPP `runtime.log`。**不要**使用 `console`/裸 stderr/`Logf`（已删除）。  
stdout 仅写协议；ready 后仍写裸 stderr 会被宿主记为 `[stderr] …`（error），应改用 `plugin.Info/Warn/Error`。

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

当目标 Brick 实例内部有状态，需要在多次命令调用之间保留上下文时，在 command handler 内使用 `ctx.OpenSession`。同一个 session 的后续 `Invoke` / `InvokeStream` 会落到同一个目标 Brick 实例；每次调用都会携带创建它且仍在执行的 command `parentRequestId`。调用 `Close()`、调用方 Brick 实例退出或宿主回收调用方时，会话会结束。

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
events, errs := session.InvokeStream("chat", map[string]any{"prompt": "继续刚才的话题"})
for event := range events {
    if event.Type == "chunk" {
        fmt.Print(event.Chunk)
    }
    if event.Type == "result" {
        _ = json.Unmarshal(event.Result, &reply)
    }
}
if err := <-errs; err != nil {
    return nil, err
}
return reply, nil
```

`WithSessionProfileID` 指定的是目标 Brick Profile ID。后续 `session.Invoke` / `session.InvokeStream` 每次都会按调用方 manifest 的 `dependencies[target].commands` 重新校验命令。

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

## 平台 Screen / Input / Screenshot API

`p.Platform.Screen.*`、`p.Platform.Input.*`、`p.Platform.Screenshot.*` 及 handler 内对应的 `ctx.Platform()` API 分别提供屏幕读取、输入自动化和交互式区域截图能力：

```go
region, err := ctx.Platform().Screenshot.SelectRegion(brickly.ScreenshotRegionOptions{})
if err != nil {
    return nil, err
}

display, err := ctx.Platform().Screen.GetPrimaryDisplay()
if err != nil {
    return nil, err
}

err = ctx.Platform().Input.KeyboardTap(brickly.KeyboardTapPayload{
    Key: "A", Modifiers: []string{"control"},
})
if err != nil {
    return nil, err
}

return map[string]any{"region": region, "display": display}, nil
```

`Screenshot` 与 `Screen` 能力需要 `os.screenshot` 权限；截图传入自定义 `outputPath` 时还需要 `fs.write`。`Input` 能力需要 `os.input`，会影响当前前台应用，应仅在用户明确触发时调用。权限拒绝及其他宿主错误会以 `BppError` 原样返回。

---

## WindowHandle（105 个反射方法）

`UI.CreateBrowserWindow` 返回的 `*WindowHandle` 封装宿主的 105 个反射方法，并提供独立的普通关闭和强制终止操作。详细契约见 [`specs/window-api.md`](../../../specs/window-api.md)。

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

### 2. 状态切换（10）

```go
win.Minimize()  win.Maximize()  win.Unmaximize()  win.Restore()
win.Hide()      win.Show()      win.ShowInactive()
win.Focus()     win.Blur()
win.SetFullScreen(true)
```

### 关闭生命周期

```go
result, err := win.Close()       // closed | prevented | pending | not-found
if err == nil && (result.Status == brickly.WindowClosePending || result.Status == brickly.WindowClosePrevented) {
    _ = win.Focus()              // 句柄仍可用
}

termination, err := win.ForceClose()
termination, err = win.Destroy() // 旧便利名，等价于 ForceClose
```

终态后 `IsClosed()` 返回 true，Runtime Map 和该句柄的全部 handler 会被清空。并发 `Close()` 共享在途请求；重复 `window.closed.eventId` 有界去重；transport 结束执行同样的本地释放。

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
// 当宿主开放了新方法、但 SDK 还没升级时，可直接走 host.ui.window.call：
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
- 当前 SDK 版本：`0.3.1`（`SdkVersion`）；BPP 协议版本：`0.2.0`；窗口使用五条 `host.ui.window.*` 消息和 105 个反射方法
- `window_protocol_generated.go` 由 Schema 生成，`TestWhitelistMatchesSchema` 额外强制方法集合完全同步

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
- `WindowHandle.SetTitle` 反射到 `host.ui.window.call`
- 窗口四态关闭、强制终止、终态去重、Map/handler 清理和 transport end
- 白名单与 `specs/bpp.schema.json` 严格相等

---

## 发布

Go SDK 通过 GitHub 仓库 tag 发布，不需要像 npm 一样上传包。发布脚本会把当前目录导出到独立仓库 `github.com/836145715/brickly-sdk-go`，提交并创建 `vX.Y.Z` tag：

```bash
cd Brickly
npm run sdk:go:publish -- 0.3.1
```

默认导出到 `../brickly-sdk-go`。如果你的独立仓库 clone 在其他位置：

```bash
npm run sdk:go:publish -- 0.3.1 --repo D:\brick-project\brickly-sdk-go
```

脚本会执行：

- `go test ./...`
- 同步 `packages/brickly-sdk-go` 到独立仓库根目录
- `git commit`
- `git tag -a v0.3.1`
- `git push origin <branch>` 和 `git push origin v0.3.1`
- `go list -m github.com/836145715/brickly-sdk-go@v0.3.1` 触发 Go module 缓存

发布后，普通开发者这样依赖：

```bash
go get github.com/836145715/brickly-sdk-go@v0.3.1
```

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

| Node SDK                                | Go SDK                                       |
| --------------------------------------- | -------------------------------------------- |
| `new BricklyRuntime({ brickId })`       | `brickly.New(brickly.Options{BrickID: ...})` |
| `brick.onCommand(id, fn)`               | `p.OnCommand(id, fn)`                        |
| `ctx.progress(v, msg)`                  | `ctx.Progress(v, msg)`                       |
| `ctx.ui.createBrowserWindow(url, opts)` | `ctx.UI().CreateBrowserWindow(url, opts)`    |
| `win.setBounds({...})`                  | `win.SetBounds(brickly.Bounds{...})`         |
| `Promise<T>`                            | `(T, error)`                                 |
| `events.on(event, fn)` 返回 unsubscribe | `Events.On(event, fn)` 返回 unsubscribe      |
| `new BppError('CODE', msg)`             | `brickly.NewBppError("CODE", msg)`           |
| `win.webContents.send(...)`             | `win.WebContents().Send(...)`                |

行为一致：序列化字节级等价、协议请求 ID 前缀 `<brickId>-<pid>-<seq>`、stdout 仅写协议、业务日志走 `runtime.log`。

### AI 对齐框架

本 SDK 是 **Follower** 实现。用 AI 跟进 Node 时请走：

- [`specs/sdk/AGENT.md`](../../../specs/sdk/AGENT.md)
- [`specs/sdk/capability-matrix.yaml`](../../../specs/sdk/capability-matrix.yaml)
- [`specs/sdk/api-mapping.yaml`](../../../specs/sdk/api-mapping.yaml)
- [`specs/sdk/prompts/follower-agent.md`](../../../specs/sdk/prompts/follower-agent.md)（`target=go`）
