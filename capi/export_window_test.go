package main

import "testing"

func TestUiCreateWindowRejectsInvalidHandle(t *testing.T) {
	_, code, message, rc := callUiCreateWindow(0, "ui/lab.html", "{}")
	if rc == 0 {
		t.Fatal("invalid handle 应失败")
	}
	if code != "INVALID_INPUT" {
		t.Fatalf("code=%s message=%s", code, message)
	}
}

func TestWindowOpsRejectInvalidHandle(t *testing.T) {
	if windowID(0) != 0 {
		t.Fatal("invalid window id 应为 0")
	}
	if !windowIsClosed(0) {
		t.Fatal("invalid window 视为已关闭")
	}
	_, code, _, rc := callWindowClose(0)
	if rc == 0 {
		t.Fatal("close invalid 应失败")
	}
	if code != "INVALID_INPUT" {
		t.Fatalf("code=%s", code)
	}
	_, code, _, rc = callWindowCall(0, "focus", "[]")
	if rc == 0 {
		t.Fatal("call invalid 应失败")
	}
	if code != "INVALID_INPUT" {
		t.Fatalf("code=%s", code)
	}
	code, _, rc = callWindowSend(0, "hello", "{}")
	if rc == 0 {
		t.Fatal("send invalid 应失败")
	}
	if code != "INVALID_INPUT" {
		t.Fatalf("code=%s", code)
	}
	windowRelease(0)
}

func TestUiCreateWindowWithoutHostFails(t *testing.T) {
	id := brickly_new()
	defer brickly_free(id)
	_, code, _, rc := callUiCreateWindow(uint64(id), "ui/lab.html", `{"width":640,"height":480,"keepAlive":true}`)
	if rc == 0 {
		t.Fatal("无 Host 时开窗应失败")
	}
	if code == "" {
		t.Fatal("应返回错误码")
	}
}
