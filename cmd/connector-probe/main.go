package main

import (
	"context"
	"fmt"
	"log"
	"os"

	runtimev1 "github.com/836145715/brickly-sdk-go/internal/grpc"
)

func main() {
	options, err := runtimev1.TakeRuntimeEnv()
	if err != nil {
		log.Fatal(err)
	}
	target := os.Getenv("BRICKLY_TARGET_BRICK_ID")
	if target == "" {
		log.Fatal("缺少 BRICKLY_TARGET_BRICK_ID")
	}
	handle, err := runtimev1.StartRuntime(options)
	if err != nil {
		log.Fatal(err)
	}
	defer handle.Close()
	client, err := runtimev1.NewHostPlatformClient(options.HostEndpoint, options.RuntimeToHostToken)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
	ctx := context.Background()
	session, err := client.Interact(ctx, target, "echo-stream", map[string]any{"via": "go"}, "probe")
	if err != nil {
		log.Fatal(err)
	}
	if err := session.Send(ctx, map[string]any{"hello": 1}); err != nil {
		log.Fatal(err)
	}
	if _, err := session.Request(ctx, map[string]any{"ping": true}); err != nil {
		log.Fatal(err)
	}
	if err := session.CloseInput(ctx); err != nil {
		log.Fatal(err)
	}
	events := 0
	for range session.Events() {
		events++
	}
	if _, err := session.Result(); err != nil {
		log.Fatal(err)
	}
	if events != 1 {
		log.Fatalf("events=%d", events)
	}
	fmt.Println("ok")
}
