package brickly

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"reflect"
	"testing"

	runtimegrpc "github.com/836145715/brickly-sdk-go/internal/grpc"
	runtimev1 "github.com/836145715/brickly-sdk-go/internal/grpc/gen"
	"google.golang.org/grpc"
)

type windowPlatformProbe struct {
	runtimev1.UnimplementedPlatformServiceServer
	requests chan *runtimev1.PlatformCallRequest
}

func (s *windowPlatformProbe) Call(_ context.Context, req *runtimev1.PlatformCallRequest) (*runtimev1.PlatformCallResponse, error) {
	s.requests <- req
	result, err := runtimegrpc.AnyToBrickValue(map[string]any{
		"windowKey": "win-1", "windowId": 1, "webContentsId": 2, "url": "panel.html",
	})
	return &runtimev1.PlatformCallResponse{Result: result}, err
}

func TestPublicWindowBindingRPC(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	probe := &windowPlatformProbe{requests: make(chan *runtimev1.PlatformCallRequest, 8)}
	runtimev1.RegisterPlatformServiceServer(server, probe)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	client, err := runtimegrpc.NewHostPlatformClient(listener.Addr().String(), "test-token")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	runtime := New()
	runtime.grpcPlatform = client
	callUI := &ScopedUI{runtime: runtime, parentRequestID: "call-1"}
	options := WindowOptions{"width": 320, "show": false}
	if _, err := callUI.CreateBrowserWindow("panel.html", options); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.UI.CreateBrowserWindow("panel.html", WindowOptions{"width": 640, "keepAlive": true}); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.UI.CreateBrowserWindow("panel.html", nil); err != nil {
		t.Fatal(err)
	}
	want := []string{
		`{"url":"panel.html","options":{"width":320,"show":false,"binding":{"kind":"call"}}}`,
		`{"url":"panel.html","options":{"width":640,"show":true,"binding":{"kind":"session","keepAlive":true}}}`,
		`{"url":"panel.html","options":{"show":true,"binding":{"kind":"session","keepAlive":false}}}`,
	}
	for _, expected := range want {
		req := <-probe.requests
		if req.Method != "ui.window.create" {
			t.Fatalf("method = %s", req.Method)
		}
		raw, err := runtimegrpc.BrickValueToJSON(req.Input)
		if err != nil {
			t.Fatal(err)
		}
		var got, expectedValue any
		if err := json.Unmarshal(raw, &got); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal([]byte(expected), &expectedValue); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, expectedValue) {
			t.Fatalf("got %s; want %s", raw, expected)
		}
	}
	if !reflect.DeepEqual(options, WindowOptions{"width": 320, "show": false}) {
		t.Fatalf("input mutated: %#v", options)
	}
	for _, invalid := range []WindowOptions{
		{"keepAlive": true}, {"keepAlive": false}, {"lifetime": "standalone"},
		{"binding": map[string]any{"kind": "session", "keepAlive": false}},
	} {
		_, err := callUI.CreateBrowserWindow("panel.html", invalid)
		var domain *BppError
		if !errors.As(err, &domain) || domain.Code != "INVALID_INPUT" {
			t.Fatalf("expected INVALID_INPUT, got %v", err)
		}
	}
	if len(probe.requests) != 0 {
		t.Fatal("invalid options reached transport")
	}
}
