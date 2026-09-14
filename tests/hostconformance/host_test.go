// Package hostconformance 用真宿主测试工件（@syllm/brickly-test-host）
// 跑 Go SDK 协议契约：真 HostGrpcServer × 真 internal/grpc 客户端。
//
// 工件未安装（node_modules/@syllm/brickly-test-host 缺失）或无 node 时整体跳过，
// 与 Python conformance 同策略。bundle 路径可用 BRICKLY_TEST_HOST_BUNDLE 覆盖。
package hostconformance

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	internalgrpc "github.com/836145715/brickly-sdk-go/internal/grpc"
	runtimev1 "github.com/836145715/brickly-sdk-go/internal/grpc/gen"
	"google.golang.org/grpc/status"
)

const kvSetPath = "/brickly.runtime.v1.BrickStorageService/KvSet"
const kvGetPath = "/brickly.runtime.v1.BrickStorageService/KvGet"

type testHost struct {
	cmd             *exec.Cmd
	stdin           io.WriteCloser
	dataEndpoint    string
	controlEndpoint string
	token           string
}

type readyInfo struct {
	DataEndpoint       string `json:"dataEndpoint"`
	ControlEndpoint    string `json:"controlEndpoint"`
	RuntimeToHostToken string `json:"runtimeToHostToken"`
}

type recordedCall struct {
	Path string `json:"path"`
	At   int64  `json:"at"`
}

func startTestHost(t *testing.T) *testHost {
	t.Helper()
	if _, err := exec.LookPath("node"); err != nil {
		t.Skip("node 不可用，跳过真宿主 conformance")
	}
	bundle := os.Getenv("BRICKLY_TEST_HOST_BUNDLE")
	if bundle == "" {
		bundle = findHostBundle()
	}
	if bundle == "" {
		t.Skip("未找到 @syllm/brickly-test-host bundle；先 npm i -D @syllm/brickly-test-host")
	}

	cmd := exec.Command("node", bundle)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatalf("stdin pipe: %v", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("stdout pipe: %v", err)
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("启动宿主进程: %v", err)
	}
	host := &testHost{cmd: cmd, stdin: stdin}
	t.Cleanup(func() { host.close() })

	lineCh := make(chan string, 1)
	go func() {
		scanner := bufio.NewScanner(stdout)
		if scanner.Scan() {
			lineCh <- scanner.Text()
		}
	}()
	select {
	case line := <-lineCh:
		var info readyInfo
		if err := json.Unmarshal([]byte(line), &info); err != nil {
			t.Fatalf("就绪行不是 JSON: %q", line)
		}
		if info.DataEndpoint == "" || info.ControlEndpoint == "" {
			t.Fatalf("就绪行缺 endpoint: %q", line)
		}
		host.dataEndpoint = info.DataEndpoint
		host.controlEndpoint = info.ControlEndpoint
		host.token = info.RuntimeToHostToken
	case <-time.After(15 * time.Second):
		t.Fatal("等待宿主就绪行超时")
	}
	return host
}

// 从 cwd 向上找 node_modules/@syllm/brickly-test-host/dist/host.cjs
// （monorepo 里命中根 node_modules 的 workspace 链接；独立仓库命中本仓 node_modules）。
func findHostBundle() string {
	dir, err := os.Getwd()
	if err != nil {
		return ""
	}
	for {
		candidate := filepath.Join(dir, "node_modules", "@syllm", "brickly-test-host", "dist", "host.cjs")
		if _, err := os.Stat(candidate); err == nil {
			return candidate
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return ""
		}
		dir = parent
	}
}

func (h *testHost) post(t *testing.T, path string, body any) {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	resp, err := http.Post("http://"+h.controlEndpoint+path, "application/json", bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("控制面 %s: %v", path, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("控制面 %s 返回 %d", path, resp.StatusCode)
	}
}

func (h *testHost) setFault(t *testing.T, path, brickCode string, count int) {
	t.Helper()
	h.post(t, "/faults", map[string]any{"path": path, "brickCode": brickCode, "count": count})
}

func (h *testHost) reset(t *testing.T) {
	t.Helper()
	h.post(t, "/reset", map[string]any{})
}

func (h *testHost) calls(t *testing.T) []recordedCall {
	t.Helper()
	resp, err := http.Get("http://" + h.controlEndpoint + "/calls")
	if err != nil {
		t.Fatalf("控制面 /calls: %v", err)
	}
	defer resp.Body.Close()
	var body struct {
		Calls []recordedCall `json:"calls"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("解析 /calls: %v", err)
	}
	return body.Calls
}

func (h *testHost) close() {
	if h.stdin != nil {
		_ = h.stdin.Close()
	}
	done := make(chan struct{})
	go func() {
		_ = h.cmd.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		_ = h.cmd.Process.Kill()
		<-done
	}
}

// 从 gRPC status details 里还原 brick 错误码（与生产 toBrickGrpcStatus 对偶）。
func assertBrickCode(t *testing.T, err error, want string) {
	t.Helper()
	if err == nil {
		t.Fatalf("期望错误 %s，实际成功", want)
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("非 gRPC status 错误: %v", err)
	}
	for _, detail := range st.Details() {
		if brickErr, ok := detail.(*runtimev1.BrickError); ok {
			if brickErr.GetCode() != want {
				t.Fatalf("brickCode=%s，期望 %s", brickErr.GetCode(), want)
			}
			return
		}
	}
	t.Fatalf("status details 中无 BrickError: %v", st)
}

func hasCall(calls []recordedCall, suffix string) bool {
	for _, call := range calls {
		if len(call.Path) >= len(suffix) && call.Path[len(call.Path)-len(suffix):] == suffix {
			return true
		}
	}
	return false
}

func TestRealHostStorageConformance(t *testing.T) {
	host := startTestHost(t)
	client, err := internalgrpc.NewHostBrickStorageClient(host.dataEndpoint, host.token)
	if err != nil {
		t.Fatalf("建客户端: %v", err)
	}
	defer client.Close()
	ctx := context.Background()

	// KV 经真实现读写
	if err := client.KvSet(ctx, "user", "name", "brickly"); err != nil {
		t.Fatalf("KvSet: %v", err)
	}
	got, found, err := client.KvGet(ctx, "user", "name")
	if err != nil || !found || got != "brickly" {
		t.Fatalf("KvGet: got=%v found=%v err=%v", got, found, err)
	}
	keys, err := client.KvList(ctx, "user", "")
	if err != nil || len(keys) != 1 || keys[0] != "name" {
		t.Fatalf("KvList: %v err=%v", keys, err)
	}

	// 真宿主 asDocument 语义：id/revision/updatedAt 合并进 data 返回
	created, err := client.CreateDoc(ctx, "user", "notes", map[string]any{"title": "hello"})
	if err != nil {
		t.Fatalf("CreateDoc: %v", err)
	}
	if created["title"] != "hello" {
		t.Fatalf("doc.title=%v", created["title"])
	}
	id, _ := created["id"].(string)
	if id == "" {
		t.Fatalf("doc.id 缺失: %v", created)
	}
	revision, _ := created["revision"].(string)
	if len(revision) < 2 || revision[:2] != "r_" {
		t.Fatalf("doc.revision 形状不对: %v", created["revision"])
	}
	if updatedAt, ok := created["updatedAt"].(float64); !ok || updatedAt <= 0 {
		t.Fatalf("doc.updatedAt 缺失: %v", created["updatedAt"])
	}

	loaded, err := client.GetDoc(ctx, "user", "notes", id)
	if err != nil || loaded == nil || loaded["title"] != "hello" || loaded["id"] != id {
		t.Fatalf("GetDoc: %v err=%v", loaded, err)
	}

	// 缺失文档 update → STORAGE_NOT_FOUND（假宿主漂移高发点）
	_, err = client.UpdateDoc(ctx, "user", "notes", "missing-doc", map[string]any{"title": "x"})
	assertBrickCode(t, err, "STORAGE_NOT_FOUND")

	// 控制面录制
	calls := host.calls(t)
	if !hasCall(calls, kvSetPath) || !hasCall(calls, kvGetPath) {
		t.Fatalf("录制缺少 KvSet/KvGet: %v", calls)
	}
}

func TestRealHostFaultInjection(t *testing.T) {
	host := startTestHost(t)
	client, err := internalgrpc.NewHostBrickStorageClient(host.dataEndpoint, host.token)
	if err != nil {
		t.Fatalf("建客户端: %v", err)
	}
	defer client.Close()
	ctx := context.Background()

	host.setFault(t, kvSetPath, "LIMIT_EXCEEDED", 1)
	err = client.KvSet(ctx, "user", "blocked", "x")
	assertBrickCode(t, err, "LIMIT_EXCEEDED")
	if found, _ := client.KvHas(ctx, "user", "blocked"); found {
		t.Fatal("被注入的写入不应产生副作用")
	}

	// 规则只拦一次，随后放行
	if err := client.KvSet(ctx, "user", "after-fault", "ok"); err != nil {
		t.Fatalf("放行后 KvSet: %v", err)
	}
	if got, _, _ := client.KvGet(ctx, "user", "after-fault"); got != "ok" {
		t.Fatalf("放行后 KvGet=%v", got)
	}

	// reset 清空规则与录制
	host.setFault(t, kvGetPath, "NOT_FOUND", 5)
	host.reset(t)
	if calls := host.calls(t); len(calls) != 0 {
		t.Fatalf("reset 后录制应为空: %v", calls)
	}
	if got, _, err := client.KvGet(ctx, "user", "after-fault"); err != nil || got != "ok" {
		t.Fatalf("reset 后 KvGet: got=%v err=%v", got, err)
	}
}
