package main

import runtimev1 "github.com/836145715/brickly-sdk-go/internal/grpc/gen"

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"syscall"

	runtimegrpc "github.com/836145715/brickly-sdk-go/internal/grpc"
)

func main() {
	options, err := runtimegrpc.TakeRuntimeEnv()
	if err != nil {
		log.Fatal(err)
	}
	options.Invoke = func(_ context.Context, commandID string, input *runtimev1.BrickValue, _ string) (*runtimev1.BrickValue, error) {
		if commandID != "fail" {
			return input, nil
		}
		raw, _ := runtimegrpc.BrickValueToJSON(input)
		var payload map[string]any
		_ = json.Unmarshal(raw, &payload)
		code, _ := payload["code"].(string)
		if code == "" {
			code = "INTERNAL"
		}
		return nil, runtimegrpc.StatusFromBrickCode(code, code+" without secret")
	}
	handle, err := runtimegrpc.StartRuntime(options)
	if err != nil {
		log.Fatal(err)
	}
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	handle.Close()
}
