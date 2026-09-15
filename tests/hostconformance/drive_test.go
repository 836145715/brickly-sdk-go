// host→runtime 驱动面 conformance：POST /spawn 铸凭据 → 环境注入起真
// brickly.Runtime → /invoke /interact /events/push /ui-handler /runtimes。
// 验证 Go SDK 在真实反向调用链下的协议行为（invoke 回包、BrickError 透传、
// interact 桥接、事件订阅送达、UI 罐头响应消费）。
package hostconformance

import (
	"encoding/json"
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
	OK      bool            `json:"ok"`
	Result  json.RawMessage `json:"result"`
	Events  []any           `json:"events"`
	DriveEr *struct {
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
