package brickly

// PlatformAPI 汇总宿主平台能力，与 Node SDK 的 brick.platform / ctx.platform 对齐。
type PlatformAPI struct {
	Screenshot *ScreenshotAPI
	Screen     *ScreenAPI
	Input      *InputAPI
	Clipboard  *ClipboardAPI
	System     *SystemAPI
}

func newPlatformAPI(runtime *Runtime, trace *TraceContext) *PlatformAPI {
	return &PlatformAPI{
		Screenshot: &ScreenshotAPI{runtime: runtime, trace: trace},
		Screen:     &ScreenAPI{runtime: runtime, trace: trace},
		Input:      &InputAPI{runtime: runtime, trace: trace},
		Clipboard:  &ClipboardAPI{runtime: runtime, trace: trace},
		System:     &SystemAPI{runtime: runtime, trace: trace},
	}
}
