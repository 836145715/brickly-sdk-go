package main

import (
	"testing"

	runtimegrpc "github.com/836145715/brickly-sdk-go/internal/grpc"
)

func TestVersionPointers(t *testing.T) {
	if goString(brickly_version()) != "0.10.0" {
		t.Fatalf("version=%s", goString(brickly_version()))
	}
	if goString(brickly_protocol_version()) != "brickly.runtime.v1" {
		t.Fatalf("protocol=%s", goString(brickly_protocol_version()))
	}
}

func TestNewFree(t *testing.T) {
	id := brickly_new()
	if id == 0 {
		t.Fatal("expected handle")
	}
	brickly_free(id)
	if lookup(uint64(id)) != nil {
		t.Fatal("free 未释放句柄")
	}
}

func TestStartRejectsMissingHostEndpoint(t *testing.T) {
	t.Setenv(runtimegrpc.HostEndpointEnv, "")
	t.Setenv(runtimegrpc.BootstrapEnv, "")
	id := brickly_new()
	defer brickly_free(id)
	if brickly_start(id) == 0 {
		t.Fatal("缺少 Host endpoint 时应返回非 0")
	}
}

func TestOnCommandRejectsEmpty(t *testing.T) {
	id := brickly_new()
	defer brickly_free(id)
	if brickly_on_command(id, nil, nil, nil) == 0 {
		t.Fatal("empty command 应失败")
	}
}
