package brickly

// SystemPathName 是 host.platform.system.getPath 支持的 Electron 路径名。
type SystemPathName string

const (
	SystemPathHome        SystemPathName = "home"
	SystemPathAppData     SystemPathName = "appData"
	SystemPathAssets      SystemPathName = "assets"
	SystemPathUserData    SystemPathName = "userData"
	SystemPathSessionData SystemPathName = "sessionData"
	SystemPathTemp        SystemPathName = "temp"
	SystemPathExe         SystemPathName = "exe"
	SystemPathModule      SystemPathName = "module"
	SystemPathDesktop     SystemPathName = "desktop"
	SystemPathDocuments   SystemPathName = "documents"
	SystemPathDownloads   SystemPathName = "downloads"
	SystemPathMusic       SystemPathName = "music"
	SystemPathPictures    SystemPathName = "pictures"
	SystemPathVideos      SystemPathName = "videos"
	SystemPathRecent      SystemPathName = "recent"
	SystemPathLogs        SystemPathName = "logs"
	SystemPathCrashDumps  SystemPathName = "crashDumps"
)

// PlatformAPI 汇总宿主平台能力。当前 Go SDK 先暴露 System，与 Node SDK 的
// brick.platform.system / ctx.platform.system 对齐。
type PlatformAPI struct {
	System    *SystemAPI
	Clipboard *ClipboardAPI
}

// ClipboardContent 是 host.platform.clipboard.setContent 的输入。
// 常见形态：{"kind":"text","text":"hello"}、{"kind":"file","paths":[]}
// 或 {"kind":"image","path":"..."}。宿主负责按 manifest 校验 os.clipboard 权限。
type ClipboardContent map[string]any

// ClipboardReadResult 是 host.platform.clipboard.readContent 的返回快照。
type ClipboardReadResult map[string]any

// ClipboardSetResult 是 host.platform.clipboard.setContent 的写入结果。
type ClipboardSetResult map[string]any

// ClipboardAPI 提供 runtime 剪贴板读写能力，对应 BPP host.platform.clipboard.*。
type ClipboardAPI struct {
	runtime *Runtime
}

// ReadContent 读取当前系统剪贴板快照。
func (c *ClipboardAPI) ReadContent() (ClipboardReadResult, error) {
	var out ClipboardReadResult
	err := c.runtime.transport.hostCall(map[string]any{
		"type": "host.platform.clipboard.readContent",
	}, &out)
	return out, err
}

// SetContent 写入系统剪贴板内容。
func (c *ClipboardAPI) SetContent(content ClipboardContent) (ClipboardSetResult, error) {
	var out ClipboardSetResult
	err := c.runtime.transport.hostCall(map[string]any{
		"type":    "host.platform.clipboard.setContent",
		"content": content,
	}, &out)
	return out, err
}

// SystemAPI 提供宿主系统能力，对应 BPP host.platform.system.*。
//
// runtime 侧仍由宿主按 manifest 权限校验；SDK 只负责序列化请求与返回值。
type SystemAPI struct {
	runtime *Runtime
}

// ShowNotification 发送系统通知。clickFeatureCode 为空时不写入请求字段。
func (s *SystemAPI) ShowNotification(body string, clickFeatureCode ...string) error {
	msg := map[string]any{
		"type": "host.platform.system.showNotification",
		"body": body,
	}
	if len(clickFeatureCode) > 0 && clickFeatureCode[0] != "" {
		msg["clickFeatureCode"] = clickFeatureCode[0]
	}
	return s.runtime.transport.hostCall(msg, nil)
}

// ShellOpenPath 使用系统默认方式打开本地路径。
func (s *SystemAPI) ShellOpenPath(fullPath string) error {
	return s.runtime.transport.hostCall(map[string]any{
		"type":     "host.platform.system.shellOpenPath",
		"fullPath": fullPath,
	}, nil)
}

// ShellTrashItem 将本地路径移入系统回收站。
func (s *SystemAPI) ShellTrashItem(fullPath string) error {
	return s.runtime.transport.hostCall(map[string]any{
		"type":     "host.platform.system.shellTrashItem",
		"fullPath": fullPath,
	}, nil)
}

// ShellShowItemInFolder 在系统文件管理器中定位指定路径。
func (s *SystemAPI) ShellShowItemInFolder(fullPath string) error {
	return s.runtime.transport.hostCall(map[string]any{
		"type":     "host.platform.system.shellShowItemInFolder",
		"fullPath": fullPath,
	}, nil)
}

// ShellOpenExternal 使用系统默认方式打开外部 URL。
func (s *SystemAPI) ShellOpenExternal(url string) error {
	return s.runtime.transport.hostCall(map[string]any{
		"type": "host.platform.system.shellOpenExternal",
		"url":  url,
	}, nil)
}

// ShellBeep 播放系统提示音。
func (s *SystemAPI) ShellBeep() error {
	return s.runtime.transport.hostCall(map[string]any{
		"type": "host.platform.system.shellBeep",
	}, nil)
}

// GetNativeID 返回宿主提供的设备标识。
func (s *SystemAPI) GetNativeID() (string, error) {
	var out string
	err := s.runtime.transport.hostCall(map[string]any{
		"type": "host.platform.system.getNativeId",
	}, &out)
	return out, err
}

// GetAppName 返回当前应用名称。
func (s *SystemAPI) GetAppName() (string, error) {
	var out string
	err := s.runtime.transport.hostCall(map[string]any{
		"type": "host.platform.system.getAppName",
	}, &out)
	return out, err
}

// GetAppVersion 返回当前应用版本。
func (s *SystemAPI) GetAppVersion() (string, error) {
	var out string
	err := s.runtime.transport.hostCall(map[string]any{
		"type": "host.platform.system.getAppVersion",
	}, &out)
	return out, err
}

// GetPath 返回指定 Electron 路径。部分平台不支持 recent，会由宿主返回 UNSUPPORTED_PLATFORM。
func (s *SystemAPI) GetPath(name SystemPathName) (string, error) {
	var out string
	err := s.runtime.transport.hostCall(map[string]any{
		"type": "host.platform.system.getPath",
		"name": string(name),
	}, &out)
	return out, err
}

// GetFileIcon 返回指定文件、扩展名或 folder 的图标 Data URL。
func (s *SystemAPI) GetFileIcon(filePath string) (string, error) {
	var out string
	err := s.runtime.transport.hostCall(map[string]any{
		"type":     "host.platform.system.getFileIcon",
		"filePath": filePath,
	}, &out)
	return out, err
}

// ReadCurrentFolderPath 读取当前前台文件管理器路径。
// macOS 支持 Finder，Windows 支持 Explorer；平台不支持或当前没有可读取的文件管理器窗口时由宿主返回错误。
func (s *SystemAPI) ReadCurrentFolderPath() (string, error) {
	var out string
	err := s.runtime.transport.hostCall(map[string]any{
		"type": "host.platform.system.readCurrentFolderPath",
	}, &out)
	return out, err
}

// ReadCurrentBrowserURL 预留接口；当前宿主未接入 native helper，会返回 UNSUPPORTED_PLATFORM。
func (s *SystemAPI) ReadCurrentBrowserURL() (string, error) {
	var out string
	err := s.runtime.transport.hostCall(map[string]any{
		"type": "host.platform.system.readCurrentBrowserUrl",
	}, &out)
	return out, err
}

// IsDev 返回宿主是否处于开发模式。
func (s *SystemAPI) IsDev() (bool, error) {
	var out bool
	err := s.runtime.transport.hostCall(map[string]any{
		"type": "host.platform.system.isDev",
	}, &out)
	return out, err
}

// IsMacOS 返回宿主是否运行于 macOS。
func (s *SystemAPI) IsMacOS() (bool, error) {
	var out bool
	err := s.runtime.transport.hostCall(map[string]any{
		"type": "host.platform.system.isMacOS",
	}, &out)
	return out, err
}

// IsWindows 返回宿主是否运行于 Windows。
func (s *SystemAPI) IsWindows() (bool, error) {
	var out bool
	err := s.runtime.transport.hostCall(map[string]any{
		"type": "host.platform.system.isWindows",
	}, &out)
	return out, err
}

// IsLinux 返回宿主是否运行于 Linux。
func (s *SystemAPI) IsLinux() (bool, error) {
	var out bool
	err := s.runtime.transport.hostCall(map[string]any{
		"type": "host.platform.system.isLinux",
	}, &out)
	return out, err
}
