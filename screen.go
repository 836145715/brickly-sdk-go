package brickly

// ScreenPoint 是物理屏幕或 DIP 坐标点。
type ScreenPoint struct {
	X float64 `json:"x"`
	Y float64 `json:"y"`
}

// ScreenRect 是物理屏幕或 DIP 坐标矩形。
type ScreenRect struct {
	X      float64 `json:"x"`
	Y      float64 `json:"y"`
	Width  float64 `json:"width"`
	Height float64 `json:"height"`
}

type ScreenshotRegionOptions map[string]any
type ScreenshotResult map[string]any
type ScreenCaptureOptions map[string]any
type ScreenCaptureResult map[string]any
type ScreenColorPickOptions map[string]any
type ScreenColorPickResult map[string]any
type ScreenDisplay map[string]any
type DesktopCaptureOptions map[string]any
type DesktopCaptureSource map[string]any

// ScreenshotAPI 提供交互式区域截图能力。
type ScreenshotAPI struct {
	runtime *Runtime
	trace   *TraceContext
}

// SelectRegion 让用户选择屏幕区域并返回截图结果。
func (s *ScreenshotAPI) SelectRegion(options ScreenshotRegionOptions) (ScreenshotResult, error) {
	msg := map[string]any{"type": "host.platform.screenshot.selectRegion"}
	if options != nil {
		msg["options"] = options
	}
	var out ScreenshotResult
	err := s.runtime.transport.hostCall(withTrace(msg, s.trace), &out)
	return out, err
}

// ScreenAPI 提供屏幕截图、显示器查询和坐标转换能力。
type ScreenAPI struct {
	runtime *Runtime
	trace   *TraceContext
}

func (s *ScreenAPI) call(messageType string, payload map[string]any, into any) error {
	msg := map[string]any{"type": messageType}
	for key, value := range payload {
		msg[key] = value
	}
	return s.runtime.transport.hostCall(withTrace(msg, s.trace), into)
}

func optionalOptions(options map[string]any) map[string]any {
	if options == nil {
		return nil
	}
	return map[string]any{"options": options}
}

func (s *ScreenAPI) CaptureRegion(options ScreenCaptureOptions) (ScreenCaptureResult, error) {
	var out ScreenCaptureResult
	err := s.call("host.platform.screen.captureRegion", optionalOptions(map[string]any(options)), &out)
	return out, err
}

func (s *ScreenAPI) PickColor(options ScreenColorPickOptions) (ScreenColorPickResult, error) {
	var out ScreenColorPickResult
	err := s.call("host.platform.screen.pickColor", optionalOptions(map[string]any(options)), &out)
	return out, err
}

func (s *ScreenAPI) GetPrimaryDisplay() (ScreenDisplay, error) {
	var out ScreenDisplay
	err := s.call("host.platform.screen.getPrimaryDisplay", nil, &out)
	return out, err
}

func (s *ScreenAPI) GetAllDisplays() ([]ScreenDisplay, error) {
	var out []ScreenDisplay
	err := s.call("host.platform.screen.getAllDisplays", nil, &out)
	return out, err
}

func (s *ScreenAPI) GetCursorScreenPoint() (ScreenPoint, error) {
	var out ScreenPoint
	err := s.call("host.platform.screen.getCursorScreenPoint", nil, &out)
	return out, err
}

func (s *ScreenAPI) GetDisplayNearestPoint(point ScreenPoint) (ScreenDisplay, error) {
	var out ScreenDisplay
	err := s.call("host.platform.screen.getDisplayNearestPoint", map[string]any{"point": point}, &out)
	return out, err
}

func (s *ScreenAPI) GetDisplayMatching(rect ScreenRect) (ScreenDisplay, error) {
	var out ScreenDisplay
	err := s.call("host.platform.screen.getDisplayMatching", map[string]any{"rect": rect}, &out)
	return out, err
}

func (s *ScreenAPI) ScreenToDIPPoint(point ScreenPoint) (ScreenPoint, error) {
	var out ScreenPoint
	err := s.call("host.platform.screen.screenToDipPoint", map[string]any{"point": point}, &out)
	return out, err
}

func (s *ScreenAPI) DIPToScreenPoint(point ScreenPoint) (ScreenPoint, error) {
	var out ScreenPoint
	err := s.call("host.platform.screen.dipToScreenPoint", map[string]any{"point": point}, &out)
	return out, err
}

func (s *ScreenAPI) ScreenToDIPRect(rect ScreenRect) (ScreenRect, error) {
	var out ScreenRect
	err := s.call("host.platform.screen.screenToDipRect", map[string]any{"rect": rect}, &out)
	return out, err
}

func (s *ScreenAPI) DIPToScreenRect(rect ScreenRect) (ScreenRect, error) {
	var out ScreenRect
	err := s.call("host.platform.screen.dipToScreenRect", map[string]any{"rect": rect}, &out)
	return out, err
}

func (s *ScreenAPI) DesktopCaptureSources(options DesktopCaptureOptions) ([]DesktopCaptureSource, error) {
	var out []DesktopCaptureSource
	err := s.call("host.platform.screen.desktopCaptureSources", map[string]any{"options": options}, &out)
	return out, err
}
