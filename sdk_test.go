package brickly

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

// —— 测试基础设施 ——
//
// 用 io.Pipe 模拟 stdin（宿主写 → SDK 读），用 *threadSafeBuffer 捕获 stdout
// （SDK 写 → 测试读）。这样就能在同一进程里跑一次完整的 host ↔ runtime 交互。

type threadSafeBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *threadSafeBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}
func (b *threadSafeBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// readNextLine 从 out 中轮询，直到读到一行 JSON 或超时。
func readNextLine(t *testing.T, out *threadSafeBuffer, consumed *int) map[string]any {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		s := out.String()
		rest := s[*consumed:]
		if i := strings.IndexByte(rest, '\n'); i >= 0 {
			line := rest[:i]
			*consumed += i + 1
			var m map[string]any
			if err := json.Unmarshal([]byte(line), &m); err != nil {
				t.Fatalf("invalid json line: %q err=%v", line, err)
			}
			return m
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for stdout line; current buffer=%q", out.String())
	return nil
}

func readLineWithin(t *testing.T, out *threadSafeBuffer, consumed *int, timeout time.Duration) (map[string]any, bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		s := out.String()
		rest := s[*consumed:]
		if i := strings.IndexByte(rest, '\n'); i >= 0 {
			line := rest[:i]
			*consumed += i + 1
			var m map[string]any
			if err := json.Unmarshal([]byte(line), &m); err != nil {
				t.Fatalf("invalid json line: %q err=%v", line, err)
			}
			return m, true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return nil, false
}

func writeLine(t *testing.T, w io.Writer, msg any) {
	t.Helper()
	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(append(data, '\n')); err != nil {
		t.Fatal(err)
	}
}

// 启动一个 Runtime 实例并返回 (brick, hostWriter, out, cleanup)。
// hostWriter 写入的内容等价于宿主把消息喂给 runtime stdin。
func newTestRuntime(t *testing.T, register func(p *Runtime)) (*Runtime, *bufio.Writer, *threadSafeBuffer) {
	t.Helper()
	stdinR, stdinW := io.Pipe()
	out := &threadSafeBuffer{}
	p := New(Options{
		BrickID: "com.test",
		Stdin:   stdinR,
		Stdout:  out,
		Stderr:  io.Discard,
	})
	if register != nil {
		register(p)
	}
	go p.Start()
	// 让 goroutine 调度上
	time.Sleep(5 * time.Millisecond)
	return p, bufio.NewWriter(stdinW), out
}

func assertBppErrorCode(t *testing.T, err error, code string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected %s, got nil", code)
	}
	var bppErr *BppError
	if !errors.As(err, &bppErr) || bppErr.Code != code {
		t.Fatalf("expected %s, got %#v", code, err)
	}
}

// ---------------------------------------------------------------------------
// 测试用例

func TestHelloAndReady(t *testing.T) {
	readyCh := make(chan struct{}, 1)
	_, in, out := newTestRuntime(t, func(p *Runtime) {
		p.OnReady(func() error {
			readyCh <- struct{}{}
			return nil
		})
	})

	writeLine(t, in, map[string]any{
		"type": "host.hello", "hostVersion": "test", "protocolVersion": "0.1.0",
	})
	in.Flush()

	consumed := 0
	msg := readNextLine(t, out, &consumed)
	if msg["type"] != "runtime.ready" {
		t.Fatalf("expected runtime.ready, got %v", msg["type"])
	}
	if msg["brickId"] != "com.test" {
		t.Fatalf("unexpected brickId: %v", msg["brickId"])
	}

	select {
	case <-readyCh:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("onReady not called")
	}
}

func TestPingPong(t *testing.T) {
	_, in, out := newTestRuntime(t, nil)
	writeLine(t, in, map[string]any{"type": "runtime.ping", "id": "ping-1"})
	in.Flush()

	consumed := 0
	msg := readNextLine(t, out, &consumed)
	if msg["type"] != "runtime.pong" || msg["id"] != "ping-1" {
		t.Fatalf("expected runtime.pong id=ping-1, got %+v", msg)
	}
}

func TestLogfWritesPlainStderrWithoutBrickIDPrefix(t *testing.T) {
	var stderr bytes.Buffer
	p := New(Options{
		BrickID: "com.test",
		Stdin:   strings.NewReader(""),
		Stdout:  io.Discard,
		Stderr:  &stderr,
	})

	p.Logf("hello %s", "world")

	if got, want := stderr.String(), "hello world\n"; got != want {
		t.Fatalf("unexpected stderr: got %q want %q", got, want)
	}
}

func TestCommandInvokeResult(t *testing.T) {
	_, in, out := newTestRuntime(t, func(p *Runtime) {
		p.OnCommand("echo", func(ctx *CommandContext, input json.RawMessage) (any, error) {
			var m map[string]any
			_ = json.Unmarshal(input, &m)
			ctx.Progress(0.5, "half")
			ctx.Output("echoed", m["text"])
			return map[string]any{"ok": true}, nil
		})
	})

	writeLine(t, in, map[string]any{
		"type": "command.invoke", "id": "req-1", "commandId": "echo",
		"input": map[string]any{"text": "hi"},
	})
	in.Flush()

	consumed := 0
	got := []string{}
	for len(got) < 3 {
		msg := readNextLine(t, out, &consumed)
		got = append(got, msg["type"].(string))
	}
	// 顺序：progress, output, result
	if got[0] != "command.progress" || got[1] != "command.output" || got[2] != "command.result" {
		t.Fatalf("unexpected message order: %v", got)
	}
}

func TestCommandError(t *testing.T) {
	_, in, out := newTestRuntime(t, func(p *Runtime) {
		p.OnCommand("boom", func(ctx *CommandContext, _ json.RawMessage) (any, error) {
			return nil, NewBppError("INVALID_INPUT", "bad input")
		})
	})

	writeLine(t, in, map[string]any{
		"type": "command.invoke", "id": "req-2", "commandId": "boom", "input": map[string]any{},
	})
	in.Flush()

	consumed := 0
	msg := readNextLine(t, out, &consumed)
	if msg["type"] != "command.error" {
		t.Fatalf("expected command.error, got %v", msg["type"])
	}
	errMap := msg["error"].(map[string]any)
	if errMap["code"] != "INVALID_INPUT" || errMap["message"] != "bad input" {
		t.Fatalf("unexpected error payload: %+v", errMap)
	}
}

func TestCommandNotFound(t *testing.T) {
	_, in, out := newTestRuntime(t, nil)
	writeLine(t, in, map[string]any{
		"type": "command.invoke", "id": "req-3", "commandId": "missing", "input": map[string]any{},
	})
	in.Flush()
	consumed := 0
	msg := readNextLine(t, out, &consumed)
	if msg["type"] != "command.error" {
		t.Fatalf("expected command.error, got %v", msg["type"])
	}
	errMap := msg["error"].(map[string]any)
	if errMap["code"] != "COMMAND_NOT_FOUND" {
		t.Fatalf("unexpected code: %v", errMap["code"])
	}
}

// TestHostCallRouting 测试 hostCall 请求-响应配对：
// SDK 发出 host.ui.createBrowserWindow，测试代码假装宿主回 host.result。
func TestHostCallRouting(t *testing.T) {
	type openResult struct {
		handle *WindowHandle
		err    error
	}
	resCh := make(chan openResult, 1)

	p, in, out := newTestRuntime(t, nil)
	// 触发握手，确保 transport 已启动
	writeLine(t, in, map[string]any{"type": "host.hello"})
	in.Flush()
	consumed := 0
	_ = readNextLine(t, out, &consumed) // runtime.ready

	go func() {
		h, err := p.UI.CreateBrowserWindow("about:blank", WindowOptions{"width": 300})
		resCh <- openResult{handle: h, err: err}
	}()

	// 读取 SDK 发出的 host.ui.createBrowserWindow
	req := readNextLine(t, out, &consumed)
	if req["type"] != "host.ui.createBrowserWindow" {
		t.Fatalf("expected host.ui.createBrowserWindow, got %v", req["type"])
	}
	reqID, _ := req["id"].(string)
	if reqID == "" {
		t.Fatal("missing request id")
	}

	// 假装宿主回复
	writeLine(t, in, map[string]any{
		"type":   "host.result",
		"id":     reqID,
		"result": map[string]any{"windowId": 42},
	})
	in.Flush()

	select {
	case r := <-resCh:
		if r.err != nil {
			t.Fatalf("CreateBrowserWindow error: %v", r.err)
		}
		if r.handle.ID != 42 {
			t.Fatalf("expected windowId=42, got %d", r.handle.ID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("CreateBrowserWindow did not return")
	}
}

// TestWindowHandleWrappers 快速验证若干白名单方法能正确序列化出 host.ui.callWindow。
func TestWindowHandleWrappers(t *testing.T) {
	p, in, out := newTestRuntime(t, nil)
	writeLine(t, in, map[string]any{"type": "host.hello"})
	in.Flush()
	consumed := 0
	_ = readNextLine(t, out, &consumed) // runtime.ready

	// 预先塞一个句柄（跳过实际 createBrowserWindow）
	h := newWindowHandle(p, 7)

	// 异步调用 SetTitle 并假装宿主回 host.result
	done := make(chan error, 1)
	go func() { done <- h.SetTitle("hello") }()

	req := readNextLine(t, out, &consumed)
	if req["type"] != "host.ui.callWindow" {
		t.Fatalf("expected host.ui.callWindow, got %v", req["type"])
	}
	if req["method"] != "setTitle" {
		t.Fatalf("expected method=setTitle, got %v", req["method"])
	}
	args := req["args"].([]any)
	if len(args) != 1 || args[0] != "hello" {
		t.Fatalf("unexpected args: %+v", args)
	}

	writeLine(t, in, map[string]any{
		"type": "host.result", "id": req["id"], "result": nil,
	})
	in.Flush()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("SetTitle err: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("SetTitle did not return")
	}
}

func TestRuntimeInvokeOutsideCommandRequiresRootAPI(t *testing.T) {
	p, in, out := newTestRuntime(t, nil)
	done := make(chan error, 1)
	var result map[string]any

	go func() {
		done <- p.Invoke("com.target", "run", nil, &result)
	}()

	consumed := 0
	if leaked, ok := readLineWithin(t, out, &consumed, 100*time.Millisecond); ok {
		if id, _ := leaked["id"].(string); id != "" {
			writeLine(t, in, map[string]any{"type": "host.result", "id": id, "result": map[string]any{"leaked": true}})
			in.Flush()
			<-done
		}
		t.Fatalf("Invoke outside command leaked protocol message: %+v", leaked)
	}

	select {
	case err := <-done:
		assertBppErrorCode(t, err, "PARENT_INVOCATION_REQUIRED")
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Invoke outside command did not return quickly")
	}
}

func TestRuntimeInvokeRootSendsHostInvokeRootWithProfile(t *testing.T) {
	type invokeResult struct {
		Value string `json:"value"`
	}
	resCh := make(chan error, 1)
	var result invokeResult

	p, in, out := newTestRuntime(t, nil)
	writeLine(t, in, map[string]any{"type": "host.hello"})
	in.Flush()
	consumed := 0
	_ = readNextLine(t, out, &consumed) // runtime.ready

	go func() {
		resCh <- p.InvokeRoot(
			"com.target",
			"run",
			map[string]any{"text": "hi"},
			&result,
			WithProfileID("work"),
		)
	}()

	req := readNextLine(t, out, &consumed)
	if req["type"] != "host.invokeRoot" {
		t.Fatalf("expected host.invokeRoot, got %v", req["type"])
	}
	if req["brickId"] != "com.target" || req["commandId"] != "run" {
		t.Fatalf("unexpected invoke target: %+v", req)
	}
	if req["profileId"] != "work" {
		t.Fatalf("expected profileId=work, got %v", req["profileId"])
	}
	if _, ok := req["parentRequestId"]; ok {
		t.Fatalf("InvokeRoot must not send parentRequestId: %+v", req)
	}

	writeLine(t, in, map[string]any{
		"type": "host.result", "id": req["id"], "result": map[string]any{"value": "ok"},
	})
	in.Flush()

	select {
	case err := <-resCh:
		if err != nil {
			t.Fatalf("InvokeRoot err: %v", err)
		}
		if result.Value != "ok" {
			t.Fatalf("expected result ok, got %q", result.Value)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("InvokeRoot did not return")
	}
}

func TestRuntimeInvokeStreamOutsideCommandRequiresParent(t *testing.T) {
	p, in, out := newTestRuntime(t, nil)

	events, errs := p.InvokeStream("com.target", "run", map[string]any{"text": "hi"}, WithProfileID("work"))

	consumed := 0
	if leaked, ok := readLineWithin(t, out, &consumed, 100*time.Millisecond); ok {
		if id, _ := leaked["id"].(string); id != "" {
			writeLine(t, in, map[string]any{"type": "host.result", "id": id, "result": nil})
			in.Flush()
			for range events {
			}
			<-errs
		}
		t.Fatalf("InvokeStream outside command leaked protocol message: %+v", leaked)
	}

	select {
	case err, ok := <-errs:
		if !ok {
			t.Fatal("InvokeStream error channel closed without error")
		}
		assertBppErrorCode(t, err, "PARENT_INVOCATION_REQUIRED")
	case <-time.After(500 * time.Millisecond):
		t.Fatal("InvokeStream outside command did not error quickly")
	}
	select {
	case _, ok := <-events:
		if ok {
			t.Fatal("InvokeStream outside command produced an event")
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("InvokeStream outside command did not close events")
	}
}

func TestCommandContextInvokeStreamReceivesChunksAndResult(t *testing.T) {
	p, in, out := newTestRuntime(t, func(p *Runtime) {
		p.OnCommand("stream-proxy", func(ctx *CommandContext, _ json.RawMessage) (any, error) {
			events, errs := ctx.InvokeStream("com.target", "run", map[string]any{"text": "hi"}, WithProfileID("work"))
			got := []InvokeStreamEvent{}
			for event := range events {
				got = append(got, event)
			}
			if err, ok := <-errs; ok && err != nil {
				return nil, err
			}
			return got, nil
		})
	})
	_ = p
	writeLine(t, in, map[string]any{"type": "host.hello"})
	writeLine(t, in, map[string]any{
		"type": "command.invoke", "id": "cmd-stream-1", "commandId": "stream-proxy", "input": nil,
	})
	in.Flush()
	consumed := 0
	_ = readNextLine(t, out, &consumed) // runtime.ready

	req := readNextLine(t, out, &consumed)
	if req["type"] != "host.invoke" {
		t.Fatalf("expected host.invoke, got %v", req["type"])
	}
	if req["stream"] != true || req["profileId"] != "work" {
		t.Fatalf("unexpected stream invoke payload: %+v", req)
	}
	if req["parentRequestId"] != "cmd-stream-1" {
		t.Fatalf("expected parentRequestId=cmd-stream-1, got %+v", req)
	}
	writeLine(t, in, map[string]any{
		"type": "host.invoke.chunk", "id": req["id"], "name": "text", "chunk": "你",
	})
	writeLine(t, in, map[string]any{
		"type": "host.invoke.chunk", "id": req["id"], "name": "text", "chunk": "好",
	})
	writeLine(t, in, map[string]any{
		"type": "host.result", "id": req["id"], "result": map[string]any{"text": "你好"},
	})
	in.Flush()

	result := readNextLine(t, out, &consumed)
	if result["type"] != "command.result" {
		t.Fatalf("expected command.result, got %+v", result)
	}
	got := result["result"].([]any)
	if len(got) != 3 {
		t.Fatalf("expected 3 stream events, got %+v", got)
	}
	first := got[0].(map[string]any)
	if first["Type"] != "chunk" || first["Name"] != "text" || first["Chunk"] != "你" {
		t.Fatalf("unexpected first chunk: %+v", first)
	}
	second := got[1].(map[string]any)
	if second["Type"] != "chunk" || second["Chunk"] != "好" {
		t.Fatalf("unexpected second chunk: %+v", second)
	}
	third := got[2].(map[string]any)
	resultPayload := third["Result"].(map[string]any)
	if third["Type"] != "result" || resultPayload["text"] != "你好" {
		t.Fatalf("unexpected result event: %+v", third)
	}
}

func TestCommandContextInvoke(t *testing.T) {
	p, in, out := newTestRuntime(t, func(p *Runtime) {
		p.OnCommand("proxy", func(ctx *CommandContext, _ json.RawMessage) (any, error) {
			var result map[string]any
			if err := ctx.Invoke("com.target", "run", nil, &result, WithProfileID("work")); err != nil {
				return nil, err
			}
			return result, nil
		})
	})
	_ = p
	writeLine(t, in, map[string]any{"type": "host.hello"})
	writeLine(t, in, map[string]any{"type": "command.invoke", "id": "cmd-1", "commandId": "proxy", "input": nil})
	in.Flush()
	consumed := 0
	_ = readNextLine(t, out, &consumed) // runtime.ready

	req := readNextLine(t, out, &consumed)
	if req["type"] != "host.invoke" {
		t.Fatalf("expected host.invoke, got %v", req["type"])
	}
	if req["profileId"] != "work" {
		t.Fatalf("expected profileId=work, got %v", req["profileId"])
	}
	if req["parentRequestId"] != "cmd-1" {
		t.Fatalf("expected parentRequestId=cmd-1, got %+v", req)
	}

	writeLine(t, in, map[string]any{
		"type": "host.result", "id": req["id"], "result": map[string]any{"proxied": true},
	})
	in.Flush()

	result := readNextLine(t, out, &consumed)
	if result["type"] != "command.result" {
		t.Fatalf("expected command.result, got %+v", result)
	}
	got := result["result"].(map[string]any)
	if got["proxied"] != true {
		t.Fatalf("unexpected command result: %+v", got)
	}
}

func TestClipboardAPISendsHostPlatformClipboardMessages(t *testing.T) {
	resultCh := make(chan error, 1)
	var readResult ClipboardReadResult
	var setResult ClipboardSetResult

	p, in, out := newTestRuntime(t, nil)
	writeLine(t, in, map[string]any{"type": "host.hello"})
	in.Flush()
	consumed := 0
	_ = readNextLine(t, out, &consumed) // runtime.ready

	if p.Platform == nil || p.Platform.Clipboard == nil {
		t.Fatal("expected Runtime.Platform.Clipboard")
	}

	go func() {
		var err error
		readResult, err = p.Platform.Clipboard.ReadContent()
		if err != nil {
			resultCh <- err
			return
		}
		setResult, err = p.Platform.Clipboard.SetContent(ClipboardContent{"kind": "text", "text": "hello"})
		resultCh <- err
	}()

	readReq := readNextLine(t, out, &consumed)
	if readReq["type"] != "host.platform.clipboard.readContent" {
		t.Fatalf("expected clipboard readContent, got %+v", readReq)
	}
	writeLine(t, in, map[string]any{
		"type": "host.result", "id": readReq["id"], "result": map[string]any{"kind": "text", "text": "old", "capturedAt": 123},
	})
	in.Flush()

	setReq := readNextLine(t, out, &consumed)
	if setReq["type"] != "host.platform.clipboard.setContent" {
		t.Fatalf("expected clipboard setContent, got %+v", setReq)
	}
	content := setReq["content"].(map[string]any)
	if content["kind"] != "text" || content["text"] != "hello" {
		t.Fatalf("unexpected clipboard set content: %+v", content)
	}
	writeLine(t, in, map[string]any{
		"type": "host.result", "id": setReq["id"], "result": map[string]any{"kind": "text", "formats": []any{"text/plain"}, "updatedAt": 456},
	})
	in.Flush()

	select {
	case err := <-resultCh:
		if err != nil {
			t.Fatalf("clipboard API err: %v", err)
		}
		if readResult["text"] != "old" || setResult["kind"] != "text" {
			t.Fatalf("unexpected clipboard results: read=%+v set=%+v", readResult, setResult)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("clipboard API calls did not return")
	}
}

func TestSystemAPISendsHostPlatformSystemMessages(t *testing.T) {
	resultCh := make(chan error, 1)
	var appName string
	var isMac bool

	p, in, out := newTestRuntime(t, nil)
	writeLine(t, in, map[string]any{"type": "host.hello"})
	in.Flush()
	consumed := 0
	_ = readNextLine(t, out, &consumed) // runtime.ready

	if p.Platform == nil || p.Platform.System != p.System {
		t.Fatal("expected Runtime.Platform.System and Runtime.System to share the same API")
	}

	go func() {
		var err error
		appName, err = p.Platform.System.GetAppName()
		if err != nil {
			resultCh <- err
			return
		}
		if err := p.System.ShowNotification("处理完成", "open-result"); err != nil {
			resultCh <- err
			return
		}
		isMac, err = p.System.IsMacOS()
		resultCh <- err
	}()

	appNameReq := readNextLine(t, out, &consumed)
	if appNameReq["type"] != "host.platform.system.getAppName" {
		t.Fatalf("expected host.platform.system.getAppName, got %+v", appNameReq)
	}
	writeLine(t, in, map[string]any{"type": "host.result", "id": appNameReq["id"], "result": "Brickly"})
	in.Flush()

	notifyReq := readNextLine(t, out, &consumed)
	if notifyReq["type"] != "host.platform.system.showNotification" {
		t.Fatalf("expected host.platform.system.showNotification, got %+v", notifyReq)
	}
	if notifyReq["body"] != "处理完成" || notifyReq["clickFeatureCode"] != "open-result" {
		t.Fatalf("unexpected notification payload: %+v", notifyReq)
	}
	writeLine(t, in, map[string]any{"type": "host.result", "id": notifyReq["id"], "result": nil})
	in.Flush()

	isMacReq := readNextLine(t, out, &consumed)
	if isMacReq["type"] != "host.platform.system.isMacOS" {
		t.Fatalf("expected host.platform.system.isMacOS, got %+v", isMacReq)
	}
	writeLine(t, in, map[string]any{"type": "host.result", "id": isMacReq["id"], "result": true})
	in.Flush()

	select {
	case err := <-resultCh:
		if err != nil {
			t.Fatalf("system API err: %v", err)
		}
		if appName != "Brickly" || !isMac {
			t.Fatalf("unexpected system results: appName=%q isMac=%v", appName, isMac)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("system API calls did not return")
	}
}

func TestSystemAPIGetPathAndErrors(t *testing.T) {
	pathCh := make(chan error, 1)
	var tempPath string

	p, in, out := newTestRuntime(t, nil)
	writeLine(t, in, map[string]any{"type": "host.hello"})
	in.Flush()
	consumed := 0
	_ = readNextLine(t, out, &consumed) // runtime.ready

	go func() {
		var err error
		tempPath, err = p.System.GetPath(SystemPathTemp)
		pathCh <- err
	}()

	pathReq := readNextLine(t, out, &consumed)
	if pathReq["type"] != "host.platform.system.getPath" || pathReq["name"] != "temp" {
		t.Fatalf("unexpected getPath request: %+v", pathReq)
	}
	writeLine(t, in, map[string]any{"type": "host.result", "id": pathReq["id"], "result": "/tmp"})
	in.Flush()

	select {
	case err := <-pathCh:
		if err != nil {
			t.Fatalf("GetPath err: %v", err)
		}
		if tempPath != "/tmp" {
			t.Fatalf("expected /tmp, got %q", tempPath)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("GetPath did not return")
	}

	errCh := make(chan error, 1)
	go func() {
		_, err := p.System.ReadCurrentFolderPath()
		errCh <- err
	}()

	unsupportedReq := readNextLine(t, out, &consumed)
	if unsupportedReq["type"] != "host.platform.system.readCurrentFolderPath" {
		t.Fatalf("expected host.platform.system.readCurrentFolderPath, got %+v", unsupportedReq)
	}
	writeLine(t, in, map[string]any{
		"type": "host.error",
		"id":   unsupportedReq["id"],
		"error": map[string]any{
			"code":    "CURRENT_FOLDER_UNAVAILABLE",
			"message": "current folder path is not available",
		},
	})
	in.Flush()

	select {
	case err := <-errCh:
		bppErr, ok := err.(*BppError)
		if !ok {
			t.Fatalf("expected *BppError, got %T", err)
		}
		if bppErr.Code != "CURRENT_FOLDER_UNAVAILABLE" {
			t.Fatalf("unexpected error code: %s", bppErr.Code)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ReadCurrentFolderPath did not return")
	}
}

func TestCommandContextSystem(t *testing.T) {
	p, in, out := newTestRuntime(t, func(p *Runtime) {
		p.OnCommand("info", func(ctx *CommandContext, _ json.RawMessage) (any, error) {
			if ctx.Platform() == nil || ctx.Platform().System != ctx.System() {
				return nil, fmt.Errorf("platform/system aliases are not aligned")
			}
			appName, err := ctx.System().GetAppName()
			if err != nil {
				return nil, err
			}
			tempPath, err := ctx.Platform().System.GetPath(SystemPathTemp)
			if err != nil {
				return nil, err
			}
			return map[string]any{"appName": appName, "temp": tempPath}, nil
		})
	})
	_ = p
	writeLine(t, in, map[string]any{"type": "host.hello"})
	writeLine(t, in, map[string]any{"type": "command.invoke", "id": "cmd-system", "commandId": "info", "input": nil})
	in.Flush()
	consumed := 0
	_ = readNextLine(t, out, &consumed) // runtime.ready

	appReq := readNextLine(t, out, &consumed)
	if appReq["type"] != "host.platform.system.getAppName" {
		t.Fatalf("expected host.platform.system.getAppName, got %+v", appReq)
	}
	writeLine(t, in, map[string]any{"type": "host.result", "id": appReq["id"], "result": "Brickly"})
	in.Flush()

	pathReq := readNextLine(t, out, &consumed)
	if pathReq["type"] != "host.platform.system.getPath" || pathReq["name"] != "temp" {
		t.Fatalf("unexpected getPath request: %+v", pathReq)
	}
	writeLine(t, in, map[string]any{"type": "host.result", "id": pathReq["id"], "result": "/tmp"})
	in.Flush()

	result := readNextLine(t, out, &consumed)
	if result["type"] != "command.result" {
		t.Fatalf("expected command.result, got %+v", result)
	}
	got := result["result"].(map[string]any)
	if got["appName"] != "Brickly" || got["temp"] != "/tmp" {
		t.Fatalf("unexpected command result: %+v", got)
	}
}

func TestRuntimeOpenSession(t *testing.T) {
	resCh := make(chan error, 1)

	p, in, out := newTestRuntime(t, nil)
	writeLine(t, in, map[string]any{"type": "host.hello"})
	in.Flush()
	consumed := 0
	_ = readNextLine(t, out, &consumed) // runtime.ready

	go func() {
		session, err := p.OpenSession("com.target", WithSessionProfileID("work"))
		if err != nil {
			resCh <- err
			return
		}
		if session.ID != "s1" || session.BrickID != "com.target" || session.ProfileID != "work" {
			resCh <- fmt.Errorf("unexpected session: %+v", session)
			return
		}
		resCh <- session.Invoke("run", map[string]any{"step": 1}, nil)
	}()

	openReq := readNextLine(t, out, &consumed)
	if openReq["type"] != "host.session.open" {
		t.Fatalf("expected host.session.open, got %v", openReq["type"])
	}
	if openReq["profileId"] != "work" {
		t.Fatalf("expected profileId=work, got %v", openReq["profileId"])
	}
	writeLine(t, in, map[string]any{
		"type": "host.result", "id": openReq["id"], "result": map[string]any{
			"sessionId": "s1", "brickId": "com.target", "profileId": "work",
		},
	})
	in.Flush()

	if leaked, ok := readLineWithin(t, out, &consumed, 100*time.Millisecond); ok {
		if id, _ := leaked["id"].(string); id != "" {
			writeLine(t, in, map[string]any{"type": "host.result", "id": id, "result": nil})
			in.Flush()
			<-resCh
		}
		t.Fatalf("session.Invoke outside command leaked protocol message: %+v", leaked)
	}

	select {
	case err := <-resCh:
		assertBppErrorCode(t, err, "PARENT_INVOCATION_REQUIRED")
	case <-time.After(500 * time.Millisecond):
		t.Fatal("session.Invoke outside command did not return quickly")
	}
}

func TestCommandContextOpenSession(t *testing.T) {
	p, in, out := newTestRuntime(t, func(p *Runtime) {
		p.OnCommand("proxy", func(ctx *CommandContext, _ json.RawMessage) (any, error) {
			session, err := ctx.OpenSession("com.target", WithSessionProfileID("work"))
			if err != nil {
				return nil, err
			}
			var result map[string]any
			if err := session.Invoke("run", nil, &result); err != nil {
				return nil, err
			}
			return result, nil
		})
	})
	_ = p
	writeLine(t, in, map[string]any{"type": "host.hello"})
	writeLine(t, in, map[string]any{"type": "command.invoke", "id": "cmd-1", "commandId": "proxy", "input": nil})
	in.Flush()
	consumed := 0
	_ = readNextLine(t, out, &consumed) // runtime.ready

	openReq := readNextLine(t, out, &consumed)
	if openReq["type"] != "host.session.open" || openReq["profileId"] != "work" {
		t.Fatalf("unexpected session open: %+v", openReq)
	}
	writeLine(t, in, map[string]any{
		"type": "host.result", "id": openReq["id"], "result": map[string]any{"sessionId": "s1", "brickId": "com.target"},
	})
	in.Flush()

	invokeReq := readNextLine(t, out, &consumed)
	if invokeReq["type"] != "host.session.invoke" {
		t.Fatalf("expected host.session.invoke, got %v", invokeReq["type"])
	}
	if invokeReq["parentRequestId"] != "cmd-1" {
		t.Fatalf("expected parentRequestId=cmd-1, got %+v", invokeReq)
	}
	writeLine(t, in, map[string]any{
		"type": "host.result", "id": invokeReq["id"], "result": map[string]any{"proxied": true},
	})
	in.Flush()

	result := readNextLine(t, out, &consumed)
	if result["type"] != "command.result" {
		t.Fatalf("expected command.result, got %+v", result)
	}
	got := result["result"].(map[string]any)
	if got["proxied"] != true {
		t.Fatalf("unexpected command result: %+v", got)
	}
}

func TestRuntimeWebContentsSendOutsideCommandRequiresParent(t *testing.T) {
	p, in, out := newTestRuntime(t, nil)
	writeLine(t, in, map[string]any{"type": "host.hello"})
	in.Flush()
	consumed := 0
	_ = readNextLine(t, out, &consumed) // runtime.ready

	createCh := make(chan struct {
		win *WindowHandle
		err error
	}, 1)
	go func() {
		win, err := p.UI.CreateBrowserWindow("x.html", nil)
		createCh <- struct {
			win *WindowHandle
			err error
		}{win: win, err: err}
	}()

	createReq := readNextLine(t, out, &consumed)
	if createReq["type"] != "host.ui.createBrowserWindow" {
		t.Fatalf("expected host.ui.createBrowserWindow, got %+v", createReq)
	}
	writeLine(t, in, map[string]any{
		"type": "host.result", "id": createReq["id"], "result": map[string]any{"windowId": 99},
	})
	in.Flush()

	var win *WindowHandle
	select {
	case res := <-createCh:
		if res.err != nil {
			t.Fatalf("CreateBrowserWindow error: %v", res.err)
		}
		win = res.win
	case <-time.After(2 * time.Second):
		t.Fatal("CreateBrowserWindow did not return")
	}

	done := make(chan error, 1)
	go func() { done <- win.WebContents().Send("ch", map[string]any{"x": 1}) }()

	if leaked, ok := readLineWithin(t, out, &consumed, 100*time.Millisecond); ok {
		if id, _ := leaked["id"].(string); id != "" {
			writeLine(t, in, map[string]any{"type": "host.result", "id": id, "result": nil})
			in.Flush()
			<-done
		}
		t.Fatalf("webContents.Send outside command leaked protocol message: %+v", leaked)
	}

	select {
	case err := <-done:
		assertBppErrorCode(t, err, "PARENT_INVOCATION_REQUIRED")
	case <-time.After(500 * time.Millisecond):
		t.Fatal("webContents.Send outside command did not return quickly")
	}

	callDone := make(chan error, 1)
	go func() { callDone <- win.Call("webContents.send", []any{"ch", map[string]any{"x": 2}}, nil) }()

	if leaked, ok := readLineWithin(t, out, &consumed, 100*time.Millisecond); ok {
		if id, _ := leaked["id"].(string); id != "" {
			writeLine(t, in, map[string]any{"type": "host.result", "id": id, "result": nil})
			in.Flush()
			<-callDone
		}
		t.Fatalf("Call(webContents.send) outside command leaked protocol message: %+v", leaked)
	}

	select {
	case err := <-callDone:
		assertBppErrorCode(t, err, "PARENT_INVOCATION_REQUIRED")
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Call(webContents.send) outside command did not return quickly")
	}
}

func TestRuntimeWebContentsSendOutsideCommandUsesPayloadRequestID(t *testing.T) {
	p, in, out := newTestRuntime(t, nil)
	writeLine(t, in, map[string]any{"type": "host.hello"})
	in.Flush()
	consumed := 0
	_ = readNextLine(t, out, &consumed) // runtime.ready

	createCh := make(chan struct {
		win *WindowHandle
		err error
	}, 1)
	go func() {
		win, err := p.UI.CreateBrowserWindow("x.html", nil)
		createCh <- struct {
			win *WindowHandle
			err error
		}{win: win, err: err}
	}()

	createReq := readNextLine(t, out, &consumed)
	writeLine(t, in, map[string]any{
		"type": "host.result", "id": createReq["id"], "result": map[string]any{"windowId": 99},
	})
	in.Flush()

	var win *WindowHandle
	select {
	case res := <-createCh:
		if res.err != nil {
			t.Fatalf("CreateBrowserWindow error: %v", res.err)
		}
		win = res.win
	case <-time.After(2 * time.Second):
		t.Fatal("CreateBrowserWindow did not return")
	}

	done := make(chan error, 1)
	go func() { done <- win.WebContents().Send("ch", map[string]any{"requestId": "ui-req-1", "x": 1}) }()

	req := readNextLine(t, out, &consumed)
	if req["type"] != "host.ui.callWindow" || req["method"] != "webContents.send" {
		t.Fatalf("expected host.ui.callWindow webContents.send, got %+v", req)
	}
	if req["parentRequestId"] != "ui-req-1" {
		t.Fatalf("expected parentRequestId ui-req-1, got %+v", req["parentRequestId"])
	}
	writeLine(t, in, map[string]any{"type": "host.result", "id": req["id"], "result": nil})
	in.Flush()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("webContents.Send error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("webContents.Send did not return")
	}
}

func TestCommandContextScopedUIWebContentsSendSendsParentOnlyForSend(t *testing.T) {
	p, in, out := newTestRuntime(t, func(p *Runtime) {
		p.OnCommand("window", func(ctx *CommandContext, _ json.RawMessage) (any, error) {
			win, err := ctx.UI().CreateBrowserWindow("x.html", nil)
			if err != nil {
				return nil, err
			}
			if err := win.WebContents().Send("ch", map[string]any{"x": 1}); err != nil {
				return nil, err
			}
			if _, err := win.WebContents().ExecuteJavaScript("1+1", nil); err != nil {
				return nil, err
			}
			if err := win.SetTitle("hello"); err != nil {
				return nil, err
			}
			return map[string]any{"windowId": win.ID}, nil
		})
	})
	_ = p
	writeLine(t, in, map[string]any{"type": "host.hello"})
	writeLine(t, in, map[string]any{"type": "command.invoke", "id": "cmd-window-1", "commandId": "window", "input": nil})
	in.Flush()
	consumed := 0
	_ = readNextLine(t, out, &consumed) // runtime.ready

	createReq := readNextLine(t, out, &consumed)
	if createReq["type"] != "host.ui.createBrowserWindow" {
		t.Fatalf("expected host.ui.createBrowserWindow, got %+v", createReq)
	}
	writeLine(t, in, map[string]any{
		"type": "host.result", "id": createReq["id"], "result": map[string]any{"windowId": 101},
	})
	in.Flush()

	sendReq := readNextLine(t, out, &consumed)
	if sendReq["type"] != "host.ui.callWindow" || sendReq["method"] != "webContents.send" {
		t.Fatalf("expected webContents.send callWindow, got %+v", sendReq)
	}
	if sendReq["parentRequestId"] != "cmd-window-1" {
		t.Fatalf("expected parentRequestId=cmd-window-1, got %+v", sendReq)
	}
	writeLine(t, in, map[string]any{"type": "host.result", "id": sendReq["id"], "result": nil})
	in.Flush()

	scriptReq := readNextLine(t, out, &consumed)
	if scriptReq["type"] != "host.ui.callWindow" || scriptReq["method"] != "webContents.executeJavaScript" {
		t.Fatalf("expected webContents.executeJavaScript callWindow, got %+v", scriptReq)
	}
	if _, ok := scriptReq["parentRequestId"]; ok {
		t.Fatalf("webContents.executeJavaScript must not send parentRequestId: %+v", scriptReq)
	}
	writeLine(t, in, map[string]any{"type": "host.result", "id": scriptReq["id"], "result": 2})
	in.Flush()

	titleReq := readNextLine(t, out, &consumed)
	if titleReq["type"] != "host.ui.callWindow" || titleReq["method"] != "setTitle" {
		t.Fatalf("expected setTitle callWindow, got %+v", titleReq)
	}
	if _, ok := titleReq["parentRequestId"]; ok {
		t.Fatalf("setTitle must not send parentRequestId: %+v", titleReq)
	}
	writeLine(t, in, map[string]any{"type": "host.result", "id": titleReq["id"], "result": nil})
	in.Flush()

	result := readNextLine(t, out, &consumed)
	if result["type"] != "command.result" {
		t.Fatalf("expected command.result, got %+v", result)
	}
	got := result["result"].(map[string]any)
	if got["windowId"] != float64(101) {
		t.Fatalf("unexpected command result: %+v", got)
	}
}

// TestEventHandlerCanHostCall 回归测试：事件 handler 内发起 hostCall 不能死锁。
//
// 历史 bug：早期 EventBus.dispatch / WindowHandle.emit 在 readLoop 的同一 goroutine
// 内同步调用用户 handler。若 handler 里发起 hostCall 等待 host.result，而该
// host.result 又必须由 readLoop 读入，readLoop 就会自己 block 自己——典型自死锁。
// 修复方案：dispatch 把每个 handler 放到独立 goroutine 中执行。
func TestEventHandlerCanHostCall(t *testing.T) {
	p, in, out := newTestRuntime(t, nil)
	writeLine(t, in, map[string]any{"type": "host.hello"})
	in.Flush()
	consumed := 0
	_ = readNextLine(t, out, &consumed) // runtime.ready

	gotTitle := make(chan string, 1)
	p.Events.On("window.message", func(payload any, _ EventEnvelope) {
		// 在 handler 内同步发起一次 hostCall，要求 SDK 不能因此死锁
		m := payload.(map[string]any)
		args := m["args"].([]any)
		_ = args
		h := newWindowHandle(p, 1)
		title, err := h.GetTitle()
		if err != nil {
			t.Errorf("GetTitle err in handler: %v", err)
			gotTitle <- ""
			return
		}
		gotTitle <- title
	})

	// 触发一条 window.message
	writeLine(t, in, map[string]any{
		"type":  "event.notify",
		"event": "window.message",
		"payload": map[string]any{
			"windowId": float64(1), "channel": "ping", "args": []any{},
		},
	})
	in.Flush()

	// SDK 在 handler 内会发出 host.ui.callWindow getTitle，测试回 result
	req := readNextLine(t, out, &consumed)
	if req["type"] != "host.ui.callWindow" || req["method"] != "getTitle" {
		t.Fatalf("expected host.ui.callWindow getTitle, got %+v", req)
	}
	writeLine(t, in, map[string]any{
		"type": "host.result", "id": req["id"], "result": "hello",
	})
	in.Flush()

	select {
	case got := <-gotTitle:
		if got != "hello" {
			t.Fatalf("expected title hello, got %q", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("event handler did not finish (likely deadlock)")
	}
}

// TestShutdownHandlerCanHostCall 回归测试：shutdown handler 内发起 hostCall 不能死锁。
//
// 与 TestEventHandlerCanHostCall 同源——若 SDK 在 readLoop 内同步执行用户
// shutdown handler，handler 里发起 hostCall 等待 host.result，readLoop 就
// 永远读不到回包。修复：handleShutdown 把整段逻辑放到独立 goroutine。
//
// 注意：本测试无法直接验证 SDK 调用 os.Exit(0)，只能验证 SDK 实际向 host
// 发出了请求并能正确处理回包；测试用 defer + recover 兜底 os.Exit 路径。
func TestShutdownHandlerCanHostCall(t *testing.T) {
	stdinR, stdinW := io.Pipe()
	out := &threadSafeBuffer{}
	p := New(Options{
		BrickID: "com.test.shutdown",
		Stdin:   stdinR,
		Stdout:  out,
		Stderr:  io.Discard,
	})

	closed := make(chan error, 1)
	p.OnShutdown(func() error {
		// 在 shutdown handler 内同步发起一次 hostCall（通用形式：listWindows）
		_, err := p.UI.ListWindows()
		closed <- err
		// 故意不让 SDK 走完 os.Exit；测试通过 closed channel 判定
		select {} // 阻塞，让外层 timeout 控制流程
	})
	go p.Start()
	time.Sleep(5 * time.Millisecond)

	in := bufio.NewWriter(stdinW)
	writeLine(t, in, map[string]any{"type": "host.hello"})
	in.Flush()
	consumed := 0
	_ = readNextLine(t, out, &consumed) // runtime.ready

	// 发送 runtime.shutdown
	writeLine(t, in, map[string]any{"type": "runtime.shutdown"})
	in.Flush()

	// SDK 在 shutdown handler 内会发出 host.ui.listWindows，测试回 result
	req := readNextLine(t, out, &consumed)
	if req["type"] != "host.ui.listWindows" {
		t.Fatalf("expected host.ui.listWindows, got %+v", req)
	}
	writeLine(t, in, map[string]any{
		"type": "host.result", "id": req["id"], "result": []any{},
	})
	in.Flush()

	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("ListWindows in shutdown handler err: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("shutdown handler did not finish (likely deadlock)")
	}
}

// TestWhitelistMatchesSchema 严格比对：本 SDK 内置的 BrickWindowMethods 必须
// 与 specs/bpp.schema.json 的 BrickWindowMethod.enum 完全一致（双向差集为空）。
// 读不到 schema 文件时跳过（例如独立分发包场景）。
func TestWhitelistMatchesSchema(t *testing.T) {
	// 去重检查
	seen := map[string]bool{}
	for _, m := range BrickWindowMethods {
		if seen[m] {
			t.Fatalf("duplicate entry in BrickWindowMethods: %s", m)
		}
		seen[m] = true
	}

	// 尝试从仓库根定位 specs/bpp.schema.json
	candidates := []string{
		"../../../specs/bpp.schema.json", // packages/brickly-sdk-go -> repo root -> specs
		"../../../../specs/bpp.schema.json",
	}
	var data []byte
	for _, p := range candidates {
		b, err := os.ReadFile(p)
		if err == nil {
			data = b
			break
		}
	}
	if data == nil {
		t.Skip("skip: specs/bpp.schema.json not found from test cwd")
		return
	}

	var schema struct {
		Definitions struct {
			BrickWindowMethod struct {
				Enum []string `json:"enum"`
			} `json:"BrickWindowMethod"`
		} `json:"definitions"`
	}
	if err := json.Unmarshal(data, &schema); err != nil {
		t.Fatalf("parse schema: %v", err)
	}
	schemaSet := map[string]bool{}
	for _, m := range schema.Definitions.BrickWindowMethod.Enum {
		schemaSet[m] = true
	}
	if len(schemaSet) == 0 {
		t.Fatal("schema BrickWindowMethod.enum empty; schema layout may have changed")
	}

	// SDK - schema
	for m := range seen {
		if !schemaSet[m] {
			t.Errorf("SDK has %q but schema doesn't", m)
		}
	}
	// schema - SDK
	for m := range schemaSet {
		if !seen[m] {
			t.Errorf("schema has %q but SDK doesn't", m)
		}
	}
}
