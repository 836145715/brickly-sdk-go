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
	var out ScreenshotResult
	err := s.runtime.platformCall("screenshot.selectRegion", options, &out)
	return out, err
}

// ScreenAPI 提供屏幕截图、显示器查询和坐标转换能力。
type ScreenAPI struct {
	runtime *Runtime
	trace   *TraceContext
}

func (s *ScreenAPI) CaptureRegion(options ScreenCaptureOptions) (ScreenCaptureResult, error) {
	var out ScreenCaptureResult
	err := s.runtime.platformCall("screen.captureRegion", options, &out)
	return out, err
}

func (s *ScreenAPI) PickColor(options ScreenColorPickOptions) (ScreenColorPickResult, error) {
	var out ScreenColorPickResult
	err := s.runtime.platformCall("screen.pickColor", options, &out)
	return out, err
}

func (s *ScreenAPI) GetPrimaryDisplay() (ScreenDisplay, error) {
	var out ScreenDisplay
	err := s.runtime.platformCall("screen.getPrimaryDisplay", nil, &out)
	return out, err
}

func (s *ScreenAPI) GetAllDisplays() ([]ScreenDisplay, error) {
	var out []ScreenDisplay
	err := s.runtime.platformCall("screen.getAllDisplays", nil, &out)
	return out, err
}

func (s *ScreenAPI) GetCursorScreenPoint() (ScreenPoint, error) {
	var out ScreenPoint
	err := s.runtime.platformCall("screen.getCursorScreenPoint", nil, &out)
	return out, err
}

func (s *ScreenAPI) GetDisplayNearestPoint(point ScreenPoint) (ScreenDisplay, error) {
	var out ScreenDisplay
	err := s.runtime.platformCall("screen.getDisplayNearestPoint", point, &out)
	return out, err
}

func (s *ScreenAPI) GetDisplayMatching(rect ScreenRect) (ScreenDisplay, error) {
	var out ScreenDisplay
	err := s.runtime.platformCall("screen.getDisplayMatching", rect, &out)
	return out, err
}

func (s *ScreenAPI) ScreenToDIPPoint(point ScreenPoint) (ScreenPoint, error) {
	var out ScreenPoint
	err := s.runtime.platformCall("screen.screenToDipPoint", point, &out)
	return out, err
}

func (s *ScreenAPI) DIPToScreenPoint(point ScreenPoint) (ScreenPoint, error) {
	var out ScreenPoint
	err := s.runtime.platformCall("screen.dipToScreenPoint", point, &out)
	return out, err
}

func (s *ScreenAPI) ScreenToDIPRect(rect ScreenRect) (ScreenRect, error) {
	var out ScreenRect
	err := s.runtime.platformCall("screen.screenToDipRect", rect, &out)
	return out, err
}

func (s *ScreenAPI) DIPToScreenRect(rect ScreenRect) (ScreenRect, error) {
	var out ScreenRect
	err := s.runtime.platformCall("screen.dipToScreenRect", rect, &out)
	return out, err
}

func (s *ScreenAPI) DesktopCaptureSources(options DesktopCaptureOptions) ([]DesktopCaptureSource, error) {
	var out []DesktopCaptureSource
	err := s.runtime.platformCall("screen.desktopCaptureSources", options, &out)
	return out, err
}
