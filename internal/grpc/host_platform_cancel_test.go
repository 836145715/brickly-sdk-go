package grpc

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
)

type hangConnector struct {
	UnimplementedBrickConnectorServiceServer
	entered chan struct{}
}

func (h *hangConnector) signalEntered() {
	select {
	case h.entered <- struct{}{}:
	default:
	}
}

func (h *hangConnector) Invoke(ctx context.Context, _ *ConnectorInvokeRequest) (*InvokeResult, error) {
	h.signalEntered()
	<-ctx.Done()
	return nil, ctx.Err()
}

func (h *hangConnector) Start(ctx context.Context, _ *ConnectorStartRequest) (*ConnectorStartResponse, error) {
	h.signalEntered()
	<-ctx.Done()
	return nil, ctx.Err()
}

func (h *hangConnector) Interact(stream grpc.BidiStreamingServer[ClientFrame, ServerFrame]) error {
	h.signalEntered()
	<-stream.Context().Done()
	return stream.Context().Err()
}

func startHangConnector(t *testing.T) (*HostPlatformClient, *hangConnector) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	hang := &hangConnector{entered: make(chan struct{}, 1)}
	server := grpc.NewServer()
	RegisterBrickConnectorServiceServer(server, hang)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	client, err := NewHostPlatformClient(listener.Addr().String(), "tok")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client, hang
}

func waitEntered(t *testing.T, hang *hangConnector) {
	t.Helper()
	select {
	case <-hang.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("出站 RPC 未进入服务端")
	}
}

func assertCanceled(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("期望 Canceled，得到 nil")
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, io.EOF) {
		return
	}
	code := status.Code(err)
	if code == codes.Canceled || code == codes.DeadlineExceeded || code == codes.Unknown {
		return
	}
	t.Fatalf("期望 Canceled，得到 %v (%s)", err, status.Code(err))
}

func TestConnectCancelsWhenContextCanceled(t *testing.T) {
	client, hang := startHangConnector(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := client.Connect(ctx, "com.b", "echo", map[string]any{"n": 1}, "call-ab")
		done <- err
	}()
	waitEntered(t, hang)
	cancel()
	select {
	case err := <-done:
		assertCanceled(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Connect 未随 ctx 取消")
	}
}

func TestConnectOnHandleCancelsWhenContextCanceled(t *testing.T) {
	client, hang := startHangConnector(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := client.ConnectOnHandle(ctx, "com.b", "echo", nil, "call-ab", "h-1")
		done <- err
	}()
	waitEntered(t, hang)
	cancel()
	select {
	case err := <-done:
		assertCanceled(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("ConnectOnHandle 未随 ctx 取消")
	}
}

func TestStartDependencyCancelsWhenContextCanceled(t *testing.T) {
	client, hang := startHangConnector(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := client.StartDependency(ctx, "com.b", "call-ab")
		done <- err
	}()
	waitEntered(t, hang)
	cancel()
	select {
	case err := <-done:
		assertCanceled(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("StartDependency 未随 ctx 取消")
	}
}

func TestInteractCancelsWhenContextCanceled(t *testing.T) {
	client, hang := startHangConnector(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := client.Interact(ctx, "com.b", "chat", map[string]any{"n": 1}, "call-ab")
		done <- err
	}()
	waitEntered(t, hang)
	cancel()
	select {
	case err := <-done:
		assertCanceled(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Interact 未随 ctx 取消")
	}
}

func TestConnectAlreadyCanceledContextFailsFast(t *testing.T) {
	client, _ := startHangConnector(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := client.Connect(ctx, "com.b", "echo", nil, "call-ab")
	assertCanceled(t, err)
}
