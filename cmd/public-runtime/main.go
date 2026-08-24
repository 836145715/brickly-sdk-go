package main

import (
	"encoding/json"
	"fmt"

	"github.com/836145715/brickly-sdk-go"
)

func main() {
	runtime := brickly.New(brickly.Options{BrickID: "com.brickly.public"})
	runtime.OnCommand("hello", func(_ *brickly.CommandContext, input json.RawMessage) (any, error) {
		var payload struct {
			Name string `json:"name"`
		}
		if err := json.Unmarshal(input, &payload); err != nil {
			return nil, err
		}
		return map[string]any{"hello": payload.Name}, nil
	})
	runtime.OnCommand("copy", func(_ *brickly.CommandContext, _ json.RawMessage) (any, error) {
		handle, err := runtime.CreateResource([]byte("grpc-bytes"), &brickly.ResourceCreateOptions{Name: "note.bin"})
		if err != nil {
			return nil, err
		}
		if handle.Ref.AccessToken != "" {
			return nil, fmt.Errorf("ResourceRef 不应包含 accessToken")
		}
		text, err := handle.Text()
		if err != nil {
			return nil, err
		}
		return map[string]any{"text": text, "sizeBytes": handle.Ref.SizeBytes}, nil
	})
	runtime.OnCommand("platform", func(_ *brickly.CommandContext, _ json.RawMessage) (any, error) {
		isWindows, err := runtime.System.IsWindows()
		if err != nil {
			return nil, err
		}
		appName, err := runtime.System.GetAppName()
		if err != nil {
			return nil, err
		}
		tempPath, err := runtime.System.GetPath(brickly.SystemPathTemp)
		if err != nil {
			return nil, err
		}
		return map[string]any{
			"isWindows": isWindows,
			"appName":   appName,
			"path":      tempPath,
		}, nil
	})
	runtime.Start()
}
