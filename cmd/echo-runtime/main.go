package main

import (
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"syscall"

	runtimev1 "github.com/836145715/brickly-sdk-go/internal/grpc"
)

func main() {
	options, err := runtimev1.TakeRuntimeEnv()
	if err != nil {
		log.Fatal(err)
	}
	options.Invoke = func(commandID string, input *runtimev1.BrickValue) (*runtimev1.BrickValue, error) {
		if commandID != "fail" {
			return input, nil
		}
		raw, _ := runtimev1.BrickValueToJSON(input)
		var payload map[string]any
		_ = json.Unmarshal(raw, &payload)
		code, _ := payload["code"].(string)
		if code == "" {
			code = "INTERNAL"
		}
		return nil, runtimev1.StatusFromBrickCode(code, code+" without secret")
	}
	handle, err := runtimev1.StartRuntime(options)
	if err != nil {
		log.Fatal(err)
	}
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	handle.Close()
}
