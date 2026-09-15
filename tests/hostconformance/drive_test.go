// host→runtime 驱动面 conformance：POST /spawn 铸凭据 → 环境注入起真
// brickly.Runtime → /invoke /interact /events/push /ui-handler /runtimes。
// 验证 Go SDK 在真实反向调用链下的协议行为（invoke 回包、BrickError 透传、
// interact 桥接、事件订阅送达、UI 罐头响应消费）。
package hostconformance

import (
	"encoding/json"
	"errors"
	"io"
	"testing"
	"time"

	brickly "github.com/836145715/brickly-sdk-go"
	runtimegrpc "github.com/836145715/brickly-sdk-go/internal/grpc"
)

const eventSubscribePath = "/brickly.runtime.v1.EventService/Subscribe"

type spawnResult struct {
	SpawnID            string `json:"spawnId"`
	BootstrapToken     string `json:"bootstrapToken"`
	RuntimeToHostToken string `json:"runtimeToHostToken"`
	HostToRuntimeToken string `json:"hostToRuntimeToken"`
	BrickID            string `json:"brickId"`
	InstanceID         string `json:"instanceId"`
}

type driveReply struct {
	OK           bool            `json:"ok"`
	InvocationID string          `json:"invocationId"`
	Result       json.RawMessage `json:"result"`
	Events       []any           `json:"events"`
	DriveEr      *struct {
		BrickCode  string `json:"brickCode"`
		Message    string `json:"message"`
		GrpcStatus int    `json:"grpcStatus"`
	} `json:"error"`
}

func (h *testHost) postJSON(t *testing.T, path string, body any, out any) {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	resp, err := postRaw("http://"+h.controlEndpoint+path, raw)
	if err != nil {
		t.Fatalf("控制面 %s: %v", path, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("控制面 %s 返回 %d: %s", path, resp.StatusCode, string(body))
	}
	if out != nil {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			t.Fatalf("解析 %s 响应: %v", path, err)
		}
	}
}

func (h *testHost) spawnBrick(t *testing.T, brickID string) spawnResult {
	t.Helper()
	var out spawnResult
	h.postJSON(t, "/spawn", map[string]any{"brickId": brickID}, &out)
	if out.SpawnID == "" || out.BootstrapToken == "" || out.RuntimeToHostToken == "" || out.HostToRuntimeToken == "" {
		t.Fatalf("/spawn 凭据不完整: %+v", out)
	}
	return out
}

func (h *testHost) invoke(t *testing.T, body map[string]any) driveReply {
	t.Helper()
	var out driveReply
	h.postJSON(t, "/invoke", body, &out)
	return out
}

func (h *testHost) interact(t *testing.T, body map[string]any) driveReply {
	t.Helper()
	var out driveReply
	h.postJSON(t, "/interact", body, &out)
	return out
}

func (h *testHost) runtimes(t *testing.T) []map[string]any {
	t.Helper()
	resp, err := getRaw("http://" + h.controlEndpoint + "/runtimes")
	if err != nil {
		t.Fatalf("控制面 /runtimes: %v", err)
	}
	defer resp.Body.Close()
	var body struct {
		Runtimes []map[string]any `json:"runtimes"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("解析 /runtimes: %v", err)
	}
	return body.Runtimes
}

func (h *testHost) uiCalls(t *testing.T) []map[string]any {
	t.Helper()
	resp, err := getRaw("http://" + h.controlEndpoint + "/ui-calls")
	if err != nil {
		t.Fatalf("控制面 /ui-calls: %v", err)
	}
	defer resp.Body.Close()
	var body struct {
		Calls []map[string]any `json:"calls"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("解析 /ui-calls: %v", err)
	}
	return body.Calls
}

func waitForCondition(t *testing.T, what string, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("等待超时：%s", what)
}

func TestRealHostDriveSurface(t *testing.T) {
	host := startTestHost(t)
	spawn := host.spawnBrick(t, "drive-go")
	if spawn.BrickID != "drive-go" {
		t.Fatalf("spawn.brickId=%s", spawn.BrickID)
	}

	// 环境注入（与真实 spawn 同形）；TakeRuntimeEnv 读完即清 token 环境变量
	t.Setenv(runtimegrpc.HostEndpointEnv, host.dataEndpoint)
	t.Setenv(runtimegrpc.BootstrapEnv, spawn.BootstrapToken)
	t.Setenv(runtimegrpc.RuntimeToHostEnv, spawn.RuntimeToHostToken)
	t.Setenv(runtimegrpc.HostToRuntimeEnv, spawn.HostToRuntimeToken)

	gotEvent := make(chan any, 4)
	rt := brickly.New()
	rt.OnCommand("echo", func(_ *brickly.CommandContext, input json.RawMessage) (any, error) {
		var v any
		if err := json.Unmarshal(input, &v); err != nil {
			return nil, err
		}
		return map[string]any{"echo": v}, nil
	})
	rt.OnCommand("boom", func(_ *brickly.CommandContext, _ json.RawMessage) (any, error) {
		return nil, brickly.NewBppError("INVALID_INPUT", "驱动面测试错误")
	})
	rt.OnCommand("open-win", func(ctx *brickly.CommandContext, _ json.RawMessage) (any, error) {
		win, err := ctx.UI().CreateBrowserWindow("https://example.com", nil)
		if err != nil {
			return nil, err
		}
		return map[string]any{"windowId": win.ID}, nil
	})
	rt.Events.On("drive:ping", func(payload any, _ brickly.EventEnvelope) {
		gotEvent <- payload
	})
	// Start 阻塞到 runtime 退出；宿主进程随测试清理关闭，goroutine 随之失效
	go func() { _ = rt.Start() }()

	// /runtimes：轮询到该 spawnId 完成注册
	waitForCondition(t, "runtime 注册", func() bool {
		for _, item := range host.runtimes(t) {
			if item["spawnId"] == spawn.SpawnID {
				return true
			}
		}
		return false
	})

	// /invoke 正向
	ok := host.invoke(t, map[string]any{
		"spawnId": spawn.SpawnID, "commandId": "echo", "input": map[string]any{"a": 1},
	})
	if !ok.OK {
		t.Fatalf("/invoke echo 失败: %+v", ok.DriveEr)
	}
	var echoed struct {
		Echo map[string]any `json:"echo"`
	}
	if err := json.Unmarshal(ok.Result, &echoed); err != nil || echoed.Echo["a"] != float64(1) {
		t.Fatalf("/invoke echo result=%s err=%v", ok.Result, err)
	}

	// /invoke 业务错误 → brickCode 透传
	bad := host.invoke(t, map[string]any{"spawnId": spawn.SpawnID, "commandId": "boom"})
	if bad.OK || bad.DriveEr == nil || bad.DriveEr.BrickCode != "INVALID_INPUT" {
		t.Fatalf("/invoke boom 应回 INVALID_INPUT: %+v", bad)
	}

	// /invoke 不存在的 spawnId → 等注册超时结构化失败
	orphan := host.invoke(t, map[string]any{
		"spawnId": "spawn-404", "commandId": "echo", "timeoutMs": 300,
	})
	if orphan.OK || orphan.DriveEr == nil {
		t.Fatalf("/invoke 未知 spawn 应失败: %+v", orphan)
	}

	// /interact 批量模式（onCommand 桥接 interact）
	it := host.interact(t, map[string]any{
		"spawnId": spawn.SpawnID, "commandId": "echo", "input": "hi",
	})
	if !it.OK {
		t.Fatalf("/interact 失败: %+v", it.DriveEr)
	}
	var itEchoed struct {
		Echo string `json:"echo"`
	}
	if err := json.Unmarshal(it.Result, &itEchoed); err != nil || itEchoed.Echo != "hi" {
		t.Fatalf("/interact result=%s err=%v", it.Result, err)
	}

	// /events/push：等订阅流建立（以控制面录制为准，禁止盲 sleep）
	waitForCondition(t, "事件订阅建立", func() bool {
		return hasCall(host.calls(t), eventSubscribePath)
	})
	host.post(t, "/events/push", map[string]any{
		"spawnId": spawn.SpawnID, "topic": "drive:ping", "payload": map[string]any{"n": 7},
	})
	select {
	case payload := <-gotEvent:
		m, _ := payload.(map[string]any)
		if m == nil || m["n"] != float64(7) {
			t.Fatalf("事件 payload=%v", payload)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("未收到 drive:ping 事件")
	}

	// /ui-handler 罐头响应 → runtime 的 ui.window.create 真实消费
	host.post(t, "/ui-handler", map[string]any{
		"method": "createBrowserWindow",
		"result": map[string]any{
			"windowKey": "w1", "windowId": 42, "webContentsId": 7, "url": "https://example.com",
		},
	})
	win := host.invoke(t, map[string]any{"spawnId": spawn.SpawnID, "commandId": "open-win"})
	if !win.OK {
		t.Fatalf("open-win 失败: %+v", win.DriveEr)
	}
	var winRes struct {
		WindowID int64 `json:"windowId"`
	}
	if err := json.Unmarshal(win.Result, &winRes); err != nil || winRes.WindowID != 42 {
		t.Fatalf("open-win result=%s err=%v", win.Result, err)
	}
	calls := host.uiCalls(t)
	if len(calls) != 1 || calls[0]["method"] != "createBrowserWindow" {
		t.Fatalf("/ui-calls=%v", calls)
	}

	// 未知 UI 方法名 → 400
	raw, _ := json.Marshal(map[string]any{"method": "notAMethod", "result": 1})
	resp, err := postRaw("http://"+host.controlEndpoint+"/ui-handler", raw)
	if err != nil {
		t.Fatalf("/ui-handler: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != 400 {
		t.Fatalf("未知 UI 方法应 400，实际 %d", resp.StatusCode)
	}

		// /reset 清驱动面状态
	host.reset(t)
	if calls := host.uiCalls(t); len(calls) != 0 {
		t.Fatalf("reset 后 /ui-calls 应为空: %v", calls)
	}
}

// startRuntimeWithEnv 注入凭据并后台启动 runtime；waitForRegistered 轮询
// /runtimes 直到该 spawnId 注册（注册成功即证明 env 已被 TakeRuntimeEnv 消费，
// 之后才能覆盖环境变量启动第二个 runtime）。
func startRuntimeWithEnv(t *testing.T, host *testHost, spawn spawnResult, bindings string) *brickly.Runtime {
	t.Helper()
	t.Setenv(runtimegrpc.HostEndpointEnv, host.dataEndpoint)
	t.Setenv(runtimegrpc.BootstrapEnv, spawn.BootstrapToken)
	t.Setenv(runtimegrpc.RuntimeToHostEnv, spawn.RuntimeToHostToken)
	t.Setenv(runtimegrpc.HostToRuntimeEnv, spawn.HostToRuntimeToken)
	if bindings != "" {
		t.Setenv(runtimegrpc.DependencyBindingsEnv, bindings)
	} else {
		t.Setenv(runtimegrpc.DependencyBindingsEnv, "")
	}
	rt := brickly.New()
	go func() { _ = rt.Start() }()
	waitForCondition(t, "runtime 注册 "+spawn.SpawnID, func() bool {
		for _, item := range host.runtimes(t) {
			if item["spawnId"] == spawn.SpawnID {
				return true
			}
		}
		return false
	})
	return rt
}

// @capability dependency.command_scoped_start
// 命令内 start 依赖 → handle.Invoke 经真 connector 路由到目标 runtime；
// /calls 录制 connector start/invoke/dispose 全链。
func TestRealHostDependencyCommandScopedStart(t *testing.T) {
	host := startTestHost(t)
	host.post(t, "/kernel/dependency", map[string]any{"commands": []string{"echo"}})
	// dep 的 spawn 身份必须与 bindings 的 BrickRef 一致（origin+brickId+version）
	var dep spawnResult
	host.postJSON(t, "/spawn", map[string]any{
		"brickId": "acme.echo-dep", "instanceId": "dep-1",
		"origin": "installed", "version": "1.2.3",
	}, &dep)
	caller := host.spawnBrick(t, "com.acme.caller")

	// 依赖目标 runtime：echo 回显输入
	t.Setenv(runtimegrpc.HostEndpointEnv, host.dataEndpoint)
	t.Setenv(runtimegrpc.BootstrapEnv, dep.BootstrapToken)
	t.Setenv(runtimegrpc.RuntimeToHostEnv, dep.RuntimeToHostToken)
	t.Setenv(runtimegrpc.HostToRuntimeEnv, dep.HostToRuntimeToken)
	depRuntime := brickly.New()
	depRuntime.OnCommand("echo", func(_ *brickly.CommandContext, input json.RawMessage) (any, error) {
		var v any
		if err := json.Unmarshal(input, &v); err != nil {
			return nil, err
		}
		return map[string]any{"dep": v}, nil
	})
	go func() { _ = depRuntime.Start() }()
	waitForCondition(t, "依赖 runtime 注册", func() bool {
		for _, item := range host.runtimes(t) {
			if item["spawnId"] == dep.SpawnID {
				return true
			}
		}
		return false
	})

	// 调用方 runtime：bindings 声明 echo-dep → acme.echo-dep（env 注入，与 .NET/Node 对齐）
	bindings := `{"echo-dep":{"brickId":"acme.echo-dep","origin":"installed","version":"1.2.3"}}`
	callerRuntime := startRuntimeWithEnv(t, host, caller, bindings)
	callerRuntime.OnCommand("use-dep", func(ctx *brickly.CommandContext, _ json.RawMessage) (any, error) {
		client, err := ctx.Dependencies().Require("echo-dep")
		if err != nil {
			return nil, err
		}
		started, err := client.Start()
		if err != nil {
			return nil, err
		}
		var out any
		if err := started.Invoke("echo", map[string]any{"x": 1}, &out); err != nil {
			return nil, err
		}
		if err := started.Dispose(); err != nil {
			return nil, err
		}
		return map[string]any{"dep": out}, nil
	})

	used := host.invoke(t, map[string]any{"spawnId": caller.SpawnID, "commandId": "use-dep"})
	if !used.OK {
		t.Fatalf("use-dep 失败: %+v", used.DriveEr)
	}
	var payload struct {
		Dep struct {
			Dep struct {
				X float64 `json:"x"`
			} `json:"dep"`
		} `json:"dep"`
	}
	if err := json.Unmarshal(used.Result, &payload); err != nil || payload.Dep.Dep.X != 1 {
		t.Fatalf("use-dep result=%s err=%v", used.Result, err)
	}

	// /calls 录制 connector 全链：start → invoke → dispose
	calls := host.calls(t)
	for _, suffix := range []string{
		"/brickly.runtime.v1.BrickConnectorService/Start",
		"/brickly.runtime.v1.BrickConnectorService/Invoke",
		"/brickly.runtime.v1.BrickConnectorService/Dispose",
	} {
		if !hasCall(calls, suffix) {
			t.Fatalf("缺少 connector 录制 %s；calls=%v", suffix, calls)
		}
	}
}

// @capability window.webcontents_send_parent
// 命令内 ScopedUI 窗口的 webContents.send 自动带当前 invocationId；
// brick 级（Session 窗）必须显式 payload.requestId，否则 PARENT_INVOCATION_REQUIRED。
func TestRealHostWindowWebContentsSendParent(t *testing.T) {
	host := startTestHost(t)
	spawn := host.spawnBrick(t, "drive-window")

	rt := brickly.New()
	rt.OnCommand("send-scoped", func(ctx *brickly.CommandContext, _ json.RawMessage) (any, error) {
		win, err := ctx.UI().CreateBrowserWindow("https://example.com", nil)
		if err != nil {
			return nil, err
		}
		if err := win.WebContents().Send("channel-a", map[string]any{"hello": 1}); err != nil {
			return nil, err
		}
		return "sent", nil
	})
	rt.OnCommand("send-unscoped", func(_ *brickly.CommandContext, _ json.RawMessage) (any, error) {
		// brick 级 UI 创建的 Session 窗不绑 command parent
		win, err := rt.UI.CreateBrowserWindow("https://example.com", nil)
		if err != nil {
			return nil, err
		}
		if err := win.WebContents().Send("channel-b", nil); err != nil {
			var bpp *brickly.BppError
			if errors.As(err, &bpp) {
				return bpp.Code, nil
			}
			return err.Error(), nil
		}
		return "NO_ERROR", nil
	})
	rt.OnCommand("send-explicit", func(_ *brickly.CommandContext, _ json.RawMessage) (any, error) {
		win, err := rt.UI.CreateBrowserWindow("https://example.com", nil)
		if err != nil {
			return nil, err
		}
		if err := win.WebContents().Send("channel-c", map[string]any{"requestId": "req-9", "v": 3}); err != nil {
			return nil, err
		}
		return "sent", nil
	})

	t.Setenv(runtimegrpc.HostEndpointEnv, host.dataEndpoint)
	t.Setenv(runtimegrpc.BootstrapEnv, spawn.BootstrapToken)
	t.Setenv(runtimegrpc.RuntimeToHostEnv, spawn.RuntimeToHostToken)
	t.Setenv(runtimegrpc.HostToRuntimeEnv, spawn.HostToRuntimeToken)
	go func() { _ = rt.Start() }()
	waitForCondition(t, "runtime 注册", func() bool {
		for _, item := range host.runtimes(t) {
			if item["spawnId"] == spawn.SpawnID {
				return true
			}
		}
		return false
	})

	host.post(t, "/ui-handler", map[string]any{
		"method": "createBrowserWindow",
		"result": map[string]any{
			"windowKey": "w1", "windowId": 42, "webContentsId": 7, "url": "https://example.com",
		},
	})
	host.post(t, "/ui-handler", map[string]any{"method": "callWindow", "result": map[string]any{"ok": true}})

	// 命令内 send：parentRequestId 自动等于本次驱动调用的 invocationId
	scoped := host.invoke(t, map[string]any{"spawnId": spawn.SpawnID, "commandId": "send-scoped"})
	if !scoped.OK {
		t.Fatalf("send-scoped 失败: %+v", scoped.DriveEr)
	}
	scopedCall := findUiCall(t, host, "webContents.send", "channel-a")
	if got := uiCallArg(t, scopedCall, 5); got != scoped.InvocationID {
		t.Fatalf("scoped parentRequestId=%v，期望 %s", got, scoped.InvocationID)
	}

	// 无 scope 窗口 + 无 requestId：拒绝
	unscoped := host.invoke(t, map[string]any{"spawnId": spawn.SpawnID, "commandId": "send-unscoped"})
	if !unscoped.OK {
		t.Fatalf("send-unscoped 失败: %+v", unscoped.DriveEr)
	}
	var unscopedCode string
	if err := json.Unmarshal(unscoped.Result, &unscopedCode); err != nil || unscopedCode != "PARENT_INVOCATION_REQUIRED" {
		t.Fatalf("send-unscoped result=%s err=%v", unscoped.Result, err)
	}

	// 无 scope 窗口 + 显式 payload.requestId：放行且透传
	explicit := host.invoke(t, map[string]any{"spawnId": spawn.SpawnID, "commandId": "send-explicit"})
	if !explicit.OK {
		t.Fatalf("send-explicit 失败: %+v", explicit.DriveEr)
	}
	explicitCall := findUiCall(t, host, "webContents.send", "channel-c")
	if got := uiCallArg(t, explicitCall, 5); got != "req-9" {
		t.Fatalf("explicit parentRequestId=%v，期望 req-9", got)
	}
}

func findUiCall(t *testing.T, host *testHost, method, channel string) map[string]any {
	t.Helper()
	for _, call := range host.uiCalls(t) {
		if call["method"] != "callWindow" {
			continue
		}
		args, ok := call["args"].([]any)
		if !ok || len(args) < 6 {
			continue
		}
		if args[3] != method {
			continue
		}
		sendArgs, ok := args[4].([]any)
		if !ok || len(sendArgs) < 1 {
			continue
		}
		if sendArgs[0] == channel {
			return call
		}
	}
	t.Fatalf("未找到 callWindow(%s, %s) 录制：%v", method, channel, host.uiCalls(t))
	return nil
}

func uiCallArg(t *testing.T, call map[string]any, index int) any {
	t.Helper()
	args, ok := call["args"].([]any)
	if !ok || len(args) <= index {
		t.Fatalf("ui-call args 形状不符: %v", call)
	}
	return args[index]
}
