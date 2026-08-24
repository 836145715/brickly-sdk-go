package brickly

import (
	"context"
	"errors"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

type memoryLifetimeHost struct {
	mu             sync.Mutex
	seq            int
	records        map[string]string
	denyStandalone bool
	stopped        []string
	stopGate       chan struct{}
}

func newMemoryLifetimeHost(denyStandalone bool) *memoryLifetimeHost {
	return &memoryLifetimeHost{records: map[string]string{}, denyStandalone: denyStandalone}
}

func (h *memoryLifetimeHost) Start(_ context.Context, _ BrickRef, options StartToolOptions) (string, error) {
	if h.denyStandalone && options.AllowStandaloneWindows {
		return "", NewBppError("PERMISSION_DENIED", "standalone 未授权")
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.seq++
	id := "h-1"
	if h.seq != 1 {
		id = "h-n"
	}
	h.records[id] = "active"
	return id, nil
}

func (h *memoryLifetimeHost) Invoke(_ context.Context, handleID, _ string, _ any) (any, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.records[handleID] != "active" {
		return nil, NewBppError("HANDLE_CLOSED", "closed")
	}
	return map[string]any{"ok": true}, nil
}

func (h *memoryLifetimeHost) Interact(ctx context.Context, handleID, commandID string, input any) (any, error) {
	return h.Invoke(ctx, handleID, commandID, input)
}

func (h *memoryLifetimeHost) Dispose(_ context.Context, handleID string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records[handleID] = "disposed"
	return nil
}

func (h *memoryLifetimeHost) Stop(_ context.Context, handleID string) error {
	if h.stopGate != nil {
		<-h.stopGate
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.stopped = append(h.stopped, handleID)
	h.records[handleID] = "stopped"
	return nil
}

func (h *memoryLifetimeHost) InvokeOnce(context.Context, BrickRef, string, any) (any, error) {
	return map[string]any{"once": true}, nil
}

func (h *memoryLifetimeHost) InteractOnce(context.Context, BrickRef, string, any) (any, error) {
	return map[string]any{"once": true}, nil
}

func TestToolLifetimeStartReady(t *testing.T) {
	sdk := NewToolSdk(newMemoryLifetimeHost(false))
	handle, err := sdk.Start(context.Background(), testTargetRef, StartToolOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if handle.State != ToolHandleActive {
		t.Fatalf("state=%s", handle.State)
	}
	if _, ok := any(handle).(interface{ LifetimeID() string }); ok {
		t.Fatal("lifetimeId must not be public")
	}
}

func TestToolLifetimeDisposeHandleClosed(t *testing.T) {
	sdk := NewToolSdk(newMemoryLifetimeHost(false))
	handle, err := sdk.Start(context.Background(), testTargetRef, StartToolOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := handle.Dispose(context.Background()); err != nil {
		t.Fatal(err)
	}
	if handle.State != ToolHandleDisposed {
		t.Fatalf("state=%s", handle.State)
	}
	_, err = handle.Invoke(context.Background(), "echo", nil)
	var bpp *BppError
	if !errors.As(err, &bpp) || bpp.Code != "HANDLE_CLOSED" {
		t.Fatalf("got %v", err)
	}
}

func TestToolLifetimeCloseAliasesDispose(t *testing.T) {
	sdk := NewToolSdk(newMemoryLifetimeHost(false))
	handle, err := sdk.Start(context.Background(), testTargetRef, StartToolOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := handle.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if handle.State != ToolHandleDisposed {
		t.Fatalf("state=%s", handle.State)
	}
	_, err = handle.Invoke(context.Background(), "echo", nil)
	var bpp *BppError
	if !errors.As(err, &bpp) || bpp.Code != "HANDLE_CLOSED" {
		t.Fatalf("got %v", err)
	}
}

func TestToolHandleDisposeAndCloseHaveNoForce(t *testing.T) {
	typ := reflect.TypeOf((*ToolHandle)(nil))
	dispose, ok := typ.MethodByName("Dispose")
	if !ok {
		t.Fatal("公开方法必须是 Dispose")
	}
	closeM, ok := typ.MethodByName("Close")
	if !ok {
		t.Fatal("若保留 Close，必须无 force 且等价 dispose")
	}
	if dispose.Type.NumIn() != 2 || closeM.Type.NumIn() != 2 {
		t.Fatal("Dispose/Close 只能是 (ctx)，禁止 Close(force bool)")
	}
	if dispose.Type.In(1).String() != "context.Context" || closeM.Type.In(1).String() != "context.Context" {
		t.Fatal("Dispose/Close 第一参数必须是 context")
	}
}

func TestToolLifetimeStopGoesThroughStopping(t *testing.T) {
	host := newMemoryLifetimeHost(false)
	host.stopGate = make(chan struct{})
	sdk := NewToolSdk(host)
	handle, err := sdk.Start(context.Background(), testTargetRef, StartToolOptions{})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		done <- handle.Stop(context.Background())
	}()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) && handle.State != ToolHandleStopping {
		time.Sleep(time.Millisecond)
	}
	if handle.State != ToolHandleStopping {
		t.Fatalf("state=%s", handle.State)
	}
	_, err = handle.Invoke(context.Background(), "echo", nil)
	var bpp *BppError
	if !errors.As(err, &bpp) || bpp.Code != "HANDLE_CLOSED" {
		t.Fatalf("got %v", err)
	}
	close(host.stopGate)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if handle.State != ToolHandleStopped {
		t.Fatalf("state=%s", handle.State)
	}
	if len(host.stopped) != 1 {
		t.Fatalf("stopped=%v", host.stopped)
	}
}

func TestToolLifetimeStoppingCanFail(t *testing.T) {
	host := newMemoryLifetimeHost(false)
	host.stopGate = make(chan struct{})
	sdk := NewToolSdk(host)
	handle, err := sdk.Start(context.Background(), testTargetRef, StartToolOptions{})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		done <- handle.Stop(context.Background())
	}()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) && handle.State != ToolHandleStopping {
		time.Sleep(time.Millisecond)
	}
	handle.State = ToolHandleFailed
	close(host.stopGate)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if handle.State != ToolHandleFailed {
		t.Fatalf("state=%s", handle.State)
	}
	_, err = handle.Invoke(context.Background(), "echo", nil)
	var bpp *BppError
	if !errors.As(err, &bpp) || bpp.Code != "RUNTIME_UNAVAILABLE" {
		t.Fatalf("got %v", err)
	}
}

func TestPublicLifetimeAPIForbidsLegacyNames(t *testing.T) {
	src, err := os.ReadFile("lifetime.go")
	if err != nil {
		t.Fatal(err)
	}
	text := string(src)
	for _, token := range []string{"UsageScope", "keepAlive", "Close(force"} {
		if strings.Contains(text, token) {
			t.Fatalf("公开 API 禁止 %s", token)
		}
	}
	handle := &ToolHandle{State: ToolHandleActive}
	if _, ok := any(handle).(interface{ LifetimeID() string }); ok {
		t.Fatal("lifetimeId must not be public")
	}
	if _, ok := any(handle).(interface{ KeepAlive() }); ok {
		t.Fatal("keepAlive must not be public")
	}
}

func TestToolLifetimeOwnerContextDisposes(t *testing.T) {
	sdk := NewToolSdk(newMemoryLifetimeHost(false))
	ctx, cancel := context.WithCancel(context.Background())
	handle, err := sdk.Start(ctx, testTargetRef, StartToolOptions{Owner: ctx})
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) && handle.State != ToolHandleDisposed {
		time.Sleep(time.Millisecond)
	}
	if handle.State != ToolHandleDisposed {
		t.Fatalf("state=%s", handle.State)
	}
}

func TestToolLifetimeStandaloneDenied(t *testing.T) {
	sdk := NewToolSdk(newMemoryLifetimeHost(true))
	_, err := sdk.Start(context.Background(), testTargetRef, StartToolOptions{AllowStandaloneWindows: true})
	var bpp *BppError
	if !errors.As(err, &bpp) || bpp.Code != "PERMISSION_DENIED" {
		t.Fatalf("got %v", err)
	}
}

func TestToolLifetimeCrashMarksFailed(t *testing.T) {
	handle := &ToolHandle{State: ToolHandleActive}
	handle.State = ToolHandleFailed
	_, err := handle.Invoke(context.Background(), "echo", nil)
	var bpp *BppError
	if !errors.As(err, &bpp) || bpp.Code != "RUNTIME_UNAVAILABLE" {
		t.Fatalf("got %v", err)
	}
}
