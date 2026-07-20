// Generated from specs/bpp.schema.json. Do not edit by hand.
package brickly

var BrickWindowMethods = []string{
	"setBounds",
	"getBounds",
	"setPosition",
	"getPosition",
	"setSize",
	"getSize",
	"setOpacity",
	"getOpacity",
	"setAlwaysOnTop",
	"isAlwaysOnTop",
	"setIgnoreMouseEvents",
	"setSkipTaskbar",
	"setTitle",
	"getTitle",
	"setResizable",
	"setMovable",
	"setFocusable",
	"setHasShadow",
	"setBackgroundColor",
	"setVisibleOnAllWorkspaces",
	"minimize",
	"maximize",
	"unmaximize",
	"restore",
	"hide",
	"show",
	"showInactive",
	"focus",
	"blur",
	"isDestroyed",
	"isVisible",
	"isFocused",
	"isMinimized",
	"isMaximized",
	"loadURL",
	"loadFile",
	"reload",
	"webContents.send",
	"webContents.executeJavaScript",
	"webContents.openDevTools",
	"webContents.closeDevTools",
	"setContentBounds",
	"getContentBounds",
	"setContentSize",
	"getContentSize",
	"setMinimumSize",
	"getMinimumSize",
	"setMaximumSize",
	"getMaximumSize",
	"setAspectRatio",
	"center",
	"moveTop",
	"moveAbove",
	"setFullScreen",
	"isFullScreen",
	"isNormal",
	"isModal",
	"getNormalBounds",
	"isResizable",
	"isMovable",
	"isFocusable",
	"setMinimizable",
	"isMinimizable",
	"setMaximizable",
	"isMaximizable",
	"setClosable",
	"isClosable",
	"setFullScreenable",
	"isFullScreenable",
	"setEnabled",
	"isEnabled",
	"hasShadow",
	"isVisibleOnAllWorkspaces",
	"setKiosk",
	"isKiosk",
	"flashFrame",
	"setProgressBar",
	"setMenuBarVisibility",
	"isMenuBarVisible",
	"setAutoHideMenuBar",
	"isMenuBarAutoHide",
	"removeMenu",
	"invalidateShadow",
	"setRepresentedFilename",
	"getRepresentedFilename",
	"setDocumentEdited",
	"isDocumentEdited",
	"webContents.toggleDevTools",
	"webContents.isDevToolsOpened",
	"webContents.goBack",
	"webContents.goForward",
	"webContents.canGoBack",
	"webContents.canGoForward",
	"webContents.getURL",
	"webContents.getTitle",
	"webContents.setZoomFactor",
	"webContents.getZoomFactor",
	"webContents.setZoomLevel",
	"webContents.getZoomLevel",
	"webContents.copy",
	"webContents.paste",
	"webContents.cut",
	"webContents.selectAll",
	"webContents.undo",
	"webContents.redo",
}

type WindowCloseStatus string

const (
	WindowCloseClosed    WindowCloseStatus = "closed"
	WindowClosePrevented WindowCloseStatus = "prevented"
	WindowClosePending   WindowCloseStatus = "pending"
	WindowCloseNotFound  WindowCloseStatus = "not-found"
)

type WindowTerminalCause string

const (
	WindowTerminalCauseWindowClosed         WindowTerminalCause = "window-closed"
	WindowTerminalCauseForceClose           WindowTerminalCause = "force-close"
	WindowTerminalCauseRenderProcessGone    WindowTerminalCause = "render-process-gone"
	WindowTerminalCauseWebContentsDestroyed WindowTerminalCause = "web-contents-destroyed"
	WindowTerminalCauseInstanceExited       WindowTerminalCause = "instance-exited"
	WindowTerminalCauseBrickClosed          WindowTerminalCause = "brick-closed"
	WindowTerminalCauseOpenFailed           WindowTerminalCause = "open-failed"
	WindowTerminalCauseOpenCancelled        WindowTerminalCause = "open-cancelled"
)

type WindowTerminalEventStatus string

const (
	WindowTerminalEventSent    WindowTerminalEventStatus = "sent"
	WindowTerminalEventSkipped WindowTerminalEventStatus = "skipped"
	WindowTerminalEventFailed  WindowTerminalEventStatus = "failed"
)

type WindowNativeStatus string

const (
	WindowNativeDestroyed        WindowNativeStatus = "destroyed"
	WindowNativeAlreadyDestroyed WindowNativeStatus = "already-destroyed"
	WindowNativeFailed           WindowNativeStatus = "failed"
)

type WindowLifecycleReleaseStatus string

const (
	WindowLifecycleReleaseReleased WindowLifecycleReleaseStatus = "released"
	WindowLifecycleReleaseQueued   WindowLifecycleReleaseStatus = "queued"
	WindowLifecycleReleaseNotBound WindowLifecycleReleaseStatus = "not-bound"
)

type WindowTerminationError struct {
	Step    string `json:"step"`
	Message string `json:"message"`
}

type WindowRequestCloseResult struct {
	Status WindowCloseStatus `json:"status"`
}

type WindowTerminationResult struct {
	Event     WindowTerminalEventStatus    `json:"event"`
	Window    WindowNativeStatus           `json:"window"`
	Lifecycle WindowLifecycleReleaseStatus `json:"lifecycle"`
	Errors    []WindowTerminationError     `json:"errors"`
}

type WindowClosedPayload struct {
	EventID   string              `json:"eventId"`
	WindowKey string              `json:"windowKey"`
	WindowID  int64               `json:"windowId"`
	Cause     WindowTerminalCause `json:"cause"`
	Forced    bool                `json:"forced"`
}
