package brickly

import (
	"testing"

	runtimegrpc "github.com/836145715/brickly-sdk-go/internal/grpc"
)

func TestStartRejectsBppFallback(t *testing.T) {
	t.Setenv(runtimegrpc.HostEndpointEnv, "")

	runtime := New()
	runtime.Start()
	if runtime.started.Load() {
		t.Fatal("Start() must not mark runtime started without BRICKLY_HOST_ENDPOINT")
	}
}
