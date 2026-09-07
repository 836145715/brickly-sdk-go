package brickly

import (
	"errors"
	"strings"
	"testing"

	runtimegrpc "github.com/836145715/brickly-sdk-go/internal/grpc"
)

func TestStartRejectsMissingHostEndpoint(t *testing.T) {
	t.Setenv(runtimegrpc.HostEndpointEnv, "")

	runtime := New()
	err := runtime.Start()
	if err == nil {
		t.Fatal("Start() must fail without BRICKLY_HOST_ENDPOINT")
	}
	var bpp *BppError
	if !errors.As(err, &bpp) || bpp.Code != "PROTOCOL_ERROR" {
		t.Fatalf("Start() error = %v, want PROTOCOL_ERROR", err)
	}
	if !strings.Contains(err.Error(), "BRICKLY_HOST_ENDPOINT") {
		t.Fatalf("Start() error = %v", err)
	}
	if runtime.started.Load() {
		t.Fatal("Start() must not mark runtime started without BRICKLY_HOST_ENDPOINT")
	}
}

func TestStartFailsWhenRuntimeEnvIncomplete(t *testing.T) {
	t.Setenv(runtimegrpc.HostEndpointEnv, "127.0.0.1:1")
	t.Setenv(runtimegrpc.BootstrapEnv, "")
	t.Setenv(runtimegrpc.RuntimeToHostEnv, "")
	t.Setenv(runtimegrpc.HostToRuntimeEnv, "")

	runtime := New()
	err := runtime.Start()
	if err == nil {
		t.Fatal("Start() must fail when bootstrap tokens are missing")
	}
	if runtime.started.Load() {
		t.Fatal("Start() must not mark runtime started when client setup fails")
	}
}
