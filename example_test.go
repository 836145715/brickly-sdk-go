package brickly_test

import (
	"encoding/json"
	"fmt"

	brickly "github.com/836145715/brickly-sdk-go"
)

// Example_minimal 展示最简 Brick 骨架：一个命令 + ready 钩子。
// 真正用作 Brick 二进制时，请在 main 包里调用 p.Start()（会阻塞）。
func Example_minimal() {
	p := brickly.New(brickly.Options{BrickID: "com.example.minimal"})

	p.OnCommand("hello", func(ctx *brickly.CommandContext, input json.RawMessage) (any, error) {
		var in struct {
			Name string `json:"name"`
		}
		_ = json.Unmarshal(input, &in)
		return map[string]string{"greet": "hello, " + in.Name}, nil
	})

	// 在真实 Brick 中：p.Start() 会阻塞到 runtime.shutdown
	_ = p
	fmt.Println("brick configured")
	// Output: brick configured
}

// Example_window 展示创建子窗口、设置属性、订阅事件。
func Example_window() {
	p := brickly.New(brickly.Options{BrickID: "com.example.pet"})

	p.OnCommand("spawn", func(ctx *brickly.CommandContext, _ json.RawMessage) (any, error) {
		win, err := ctx.UI().CreateBrowserWindow("ui/pet.html", brickly.WindowOptions{
			"width":       200,
			"height":      200,
			"frame":       false,
			"transparent": true,
			"alwaysOnTop": true,
		})
		if err != nil {
			return nil, err
		}

		// 顶置 + 设置透明度
		_ = win.SetAlwaysOnTop(true, "screen-saver")
		_ = win.SetOpacity(0.9)

		// 订阅关闭事件，记录 id
		win.On("closed", func(payload map[string]any) {
			_ = ctx.Events().Publish("pet.closed", payload)
		})

		return map[string]any{"windowId": win.ID}, nil
	})

	_ = p
	fmt.Println("window brick configured")
	// Output: window brick configured
}
