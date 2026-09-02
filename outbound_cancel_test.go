package brickly

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	runtimegrpc "github.com/836145715/brickly-sdk-go/internal/grpc"
)

type hangConnector struct {
	runtimegrpc.UnimplementedBrickConnectorServiceServer
	entered chan struct{}
}

func (h *hangConnector) signalEntered() {
	select {
	case h.entered <- struct{}{}:
	default:
	}
}

func (h *hangConnector) Invoke(ctx context.Context, _ *runtimegrpc.ConnectorInvokeRequest) (*runtimegrpc.InvokeResult, error) {
	h.signalEntered()
	<-ctx.Done()
	return nil, ctx.Err()
}

func (h *hangConnector) Start(ctx context.Context, _ *runtimegrpc.ConnectorStartRequest) (*runtimegrpc.ConnectorStartResponse, error) {
	h.signalEntered()
	<-ctx.Done()
	return nil, ctx.Err()
}

func (h *hangConnector) Interact(stream grpc.BidiStreamingServer[runtimegrpc.ClientFrame, runtimegrpc.ServerFrame]) error {
	h.signalEntered()
	<-stream.Context().Done()
	return stream.Context().Err()
}

func startRuntimeHangConnector(t *testing.T) (*Runtime, *hangConnector) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	hang := &hangConnector{entered: make(chan struct{}, 1)}
	server := grpc.NewServer()
	runtimegrpc.RegisterBrickConnectorServiceServer(server, hang)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	client, err := runtimegrpc.NewHostPlatformClient(listener.Addr().String(), "tok")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	p := New()
	p.grpcPlatform = client
	return p, hang
}

func waitHangEntered(t *testing.T, hang *hangConnector) {
	t.Helper()
	select {
	case <-hang.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("出站 RPC 未进入服务端")
	}
}

func assertRPCCanceled(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("期望取消错误")
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, io.EOF) || err == io.EOF {
		return
	}
	code := status.Code(err)
	if code == codes.Canceled || code == codes.DeadlineExceeded || code == codes.Unknown {
		return
	}
	t.Fatalf("期望 Canceled，得到 %v (%s)", err, code)
}

func TestConnectorInvokeCancelsWhenCommandContextCanceled(t *testing.T) {
	p, hang := startRuntimeHangConnector(t)
	parent, cancel := context.WithCancel(context.Background())
	cmd := newCommandContext(p, "call-ab", "run", CommandInvocationContext{Source: "unknown"}, nil, parent)
	p.enterCommand("call-ab", cmd.Context())
	defer p.leaveCommand()

	done := make(chan error, 1)
	go func() {
		done <- p.connectorInvoke("com.b", "echo", map[string]any{"n": 1}, "call-ab", nil)
	}()
	waitHangEntered(t, hang)
	cancel()
	select {
	case err := <-done:
		assertRPCCanceled(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("命令取消后出站 Invoke 仍未返回")
	}
}

func TestConnectorInvokeOnHandleCancelsWhenCommandContextCanceled(t *testing.T) {
	p, hang := startRuntimeHangConnector(t)
	parent, cancel := context.WithCancel(context.Background())
	cmd := newCommandContext(p, "call-ab", "run", CommandInvocationContext{Source: "unknown"}, nil, parent)
	p.enterCommand("call-ab", cmd.Context())
	defer p.leaveCommand()

	done := make(chan error, 1)
	go func() {
		done <- p.connectorInvokeOnHandle("com.b", "echo", nil, "call-ab", "h-1", nil)
	}()
	waitHangEntered(t, hang)
	cancel()
	select {
	case err := <-done:
		assertRPCCanceled(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Handle.Invoke 未随命令取消")
	}
}

func TestStartDependencyCancelsWhenCommandContextCanceled(t *testing.T) {
	p, hang := startRuntimeHangConnector(t)
	parent, cancel := context.WithCancel(context.Background())
	cmd := newCommandContext(p, "call-ab", "run", CommandInvocationContext{Source: "unknown"}, nil, parent)
	p.enterCommand("call-ab", cmd.Context())
	defer p.leaveCommand()

	done := make(chan error, 1)
	go func() {
		_, err := p.startDependency("other", testTargetRef)
		done <- err
	}()
	waitHangEntered(t, hang)
	cancel()
	select {
	case err := <-done:
		assertRPCCanceled(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("start 未随命令取消")
	}
}

func TestConnectorInvokeOutsideCommandDoesNotFollowOldCommandCtx(t *testing.T) {
	p, hang := startRuntimeHangConnector(t)
	parent, cancel := context.WithCancel(context.Background())
	cmd := newCommandContext(p, "call-ab", "run", CommandInvocationContext{Source: "unknown"}, nil, parent)
	p.enterCommand("call-ab", cmd.Context())
	p.leaveCommand()
	cancel()

	ctx, stop := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer stop()
	done := make(chan error, 1)
	go func() {
		done <- p.connectorInvoke("com.b", "echo", nil, "", nil)
	}()
	waitHangEntered(t, hang)
	select {
	case err := <-done:
		t.Fatalf("命令外出站不应被已结束命令的 ctx 拆掉，得到 %v", err)
	case <-ctx.Done():
		// 挂起直到测试超时，说明没有误绑旧命令 ctx
	}
}

func TestConnectorInteractCancelsWhenExplicitContextCanceled(t *testing.T) {
	p, hang := startRuntimeHangConnector(t)
	parent, cancelParent := context.WithCancel(context.Background())
	defer cancelParent()
	cmd := newCommandContext(p, "call-ab", "run", CommandInvocationContext{Source: "unknown"}, nil, parent)
	p.enterCommand("call-ab", cmd.Context())
	defer p.leaveCommand()

	explicit, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := p.connectorInteract(explicit, "com.b", "chat", map[string]any{"n": 1}, "call-ab", "")
		done <- err
	}()
	waitHangEntered(t, hang)
	cancel()
	select {
	case err := <-done:
		assertRPCCanceled(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Interact 未随作者 ctx 取消")
	}
}

func TestConnectorInteractCancelsWhenCommandContextCanceled(t *testing.T) {
	p, hang := startRuntimeHangConnector(t)
	parent, cancel := context.WithCancel(context.Background())
	cmd := newCommandContext(p, "call-ab", "run", CommandInvocationContext{Source: "unknown"}, nil, parent)
	p.enterCommand("call-ab", cmd.Context())
	defer p.leaveCommand()

	done := make(chan error, 1)
	go func() {
		_, err := p.connectorInteract(context.Background(), "com.b", "chat", map[string]any{"n": 1}, "call-ab", "")
		done <- err
	}()
	waitHangEntered(t, hang)
	cancel()
	select {
	case err := <-done:
		assertRPCCanceled(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Interact 未随命令 ctx 取消")
	}
}
