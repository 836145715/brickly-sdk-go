package brickly

// ProtocolVersion 是当前 SDK 实现的 BPP 协议版本。
// 保持与 packages/brickly-sdk-node/src/protocol.ts 一致。
const ProtocolVersion = "0.1.0"

// BrickWindowMethods 是当前 SDK 已知的白名单方法清单（v0.2，107 个）。
// 真相源是 specs/bpp.schema.json 的 BrickWindowMethod enum；此数组仅用于
// 运行时自检、文档生成和调试。WindowHandle 上的每个强类型方法都对应其中一项。
var BrickWindowMethods = []string{
	// 1. 几何 / 位置（17）
	"setBounds", "getBounds", "setContentBounds", "getContentBounds", "getNormalBounds",
	"setPosition", "getPosition", "setSize", "getSize", "setContentSize", "getContentSize",
	"setMinimumSize", "getMinimumSize", "setMaximumSize", "getMaximumSize",
	"setAspectRatio", "center",

	// 2. 状态切换（12）
	"minimize", "maximize", "unmaximize", "restore", "hide", "show", "showInactive",
	"focus", "blur", "setFullScreen", "destroy", "close",

	// 3. 状态查询（23，全部返回 bool）
	"isDestroyed", "isVisible", "isFocused", "isMinimized", "isMaximized", "isFullScreen",
	"isNormal", "isModal", "isResizable", "isMovable", "isFocusable", "isMinimizable",
	"isMaximizable", "isClosable", "isFullScreenable", "isEnabled", "hasShadow",
	"isAlwaysOnTop", "isVisibleOnAllWorkspaces", "isKiosk", "isMenuBarVisible",
	"isMenuBarAutoHide", "isDocumentEdited",

	// 4. 视觉属性（9）
	"setOpacity", "getOpacity", "setBackgroundColor", "setHasShadow", "setTitle", "getTitle",
	"invalidateShadow", "flashFrame", "setProgressBar",

	// 5. 层叠 / 鼠标 / 任务栏（7）
	"setAlwaysOnTop", "setIgnoreMouseEvents", "setSkipTaskbar", "setVisibleOnAllWorkspaces",
	"moveTop", "moveAbove", "setKiosk",

	// 6. 能力开关（8）
	"setResizable", "setMovable", "setFocusable", "setMinimizable", "setMaximizable",
	"setClosable", "setFullScreenable", "setEnabled",

	// 7. 菜单栏（3）
	"setMenuBarVisibility", "setAutoHideMenuBar", "removeMenu",

	// 8. macOS 文档窗口（3）
	"setRepresentedFilename", "getRepresentedFilename", "setDocumentEdited",

	// 9. 内容加载（3）
	"loadURL", "loadFile", "reload",

	// 10. WebContents（22）
	"webContents.send", "webContents.executeJavaScript",
	"webContents.openDevTools", "webContents.closeDevTools", "webContents.toggleDevTools",
	"webContents.isDevToolsOpened",
	"webContents.goBack", "webContents.goForward",
	"webContents.canGoBack", "webContents.canGoForward",
	"webContents.getURL", "webContents.getTitle",
	"webContents.setZoomFactor", "webContents.getZoomFactor",
	"webContents.setZoomLevel", "webContents.getZoomLevel",
	"webContents.copy", "webContents.paste", "webContents.cut",
	"webContents.selectAll", "webContents.undo", "webContents.redo",
}
