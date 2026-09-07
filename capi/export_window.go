package main

/*
#include "callback.h"
#include <stdlib.h>
*/
import "C"
import (
	"encoding/json"
	"unsafe"

	brickly "github.com/836145715/brickly-sdk-go"
)

func lookupWindow(id C.uint64_t) (*slot, *brickly.WindowHandle, error) {
	s := lookup(uint64(id))
	if s == nil || s.win == nil {
		return nil, nil, invalidHandle()
	}
	return s, s.win, nil
}

func decodeWindowOptions(raw string) (brickly.WindowOptions, error) {
	if raw == "" {
		return brickly.WindowOptions{}, nil
	}
	value, err := decodeJSONString(raw)
	if err != nil {
		return nil, err
	}
	if value == nil {
		return brickly.WindowOptions{}, nil
	}
	fields, ok := value.(map[string]any)
	if !ok {
		return nil, brickly.NewBppError("INVALID_INPUT", "window options 必须是 JSON 对象")
	}
	return brickly.WindowOptions(fields), nil
}

func decodeCallArgs(raw string) ([]any, error) {
	if raw == "" || raw == "null" {
		return []any{}, nil
	}
	value, err := decodeJSONString(raw)
	if err != nil {
		return nil, err
	}
	if value == nil {
		return []any{}, nil
	}
	args, ok := value.([]any)
	if !ok {
		return nil, brickly.NewBppError("INVALID_INPUT", "window call args 必须是 JSON 数组")
	}
	return args, nil
}

func createWindow(s *slot, url, optionsJSON string) (*brickly.WindowHandle, error) {
	if s == nil {
		return nil, invalidHandle()
	}
	opts, err := decodeWindowOptions(optionsJSON)
	if err != nil {
		return nil, err
	}
	if s.ctx != nil {
		scoped, err := s.ctx.UI().CreateBrowserWindow(url, opts)
		if err != nil {
			return nil, err
		}
		return scoped.WindowHandle, nil
	}
	if s.rt != nil && s.rt.UI != nil {
		return s.rt.UI.CreateBrowserWindow(url, opts)
	}
	return nil, invalidHandle()
}

//export brickly_ui_create_window
func brickly_ui_create_window(id C.uint64_t, url *C.char, optionsJSON *C.char, out *C.uint64_t, code **C.char, message **C.char) C.int {
	s := lookup(uint64(id))
	if s == nil || out == nil {
		return failOut(invalidHandle(), code, message)
	}
	handle, err := createWindow(s, goString(url), goString(optionsJSON))
	if err != nil {
		return failOut(err, code, message)
	}
	*out = C.uint64_t(alloc(&slot{rt: s.rt, win: handle}))
	return 0
}

//export brickly_window_id
func brickly_window_id(id C.uint64_t) C.int64_t {
	_, win, err := lookupWindow(id)
	if err != nil {
		return 0
	}
	return C.int64_t(win.ID)
}

//export brickly_window_is_closed
func brickly_window_is_closed(id C.uint64_t) C.int {
	_, win, err := lookupWindow(id)
	if err != nil || win.IsClosed() {
		return 1
	}
	return 0
}

//export brickly_window_close
func brickly_window_close(id C.uint64_t, outJSON **C.char, code **C.char, message **C.char) C.int {
	_, win, err := lookupWindow(id)
	if err != nil {
		return failOut(err, code, message)
	}
	result, err := win.Close()
	if err != nil {
		return failOut(err, code, message)
	}
	return failOut(writeJSONValue(result, outJSON), code, message)
}

//export brickly_window_release
func brickly_window_release(id C.uint64_t) {
	drop(uint64(id))
}

//export brickly_window_call
func brickly_window_call(id C.uint64_t, method *C.char, argsJSON *C.char, outJSON **C.char, code **C.char, message **C.char) C.int {
	_, win, err := lookupWindow(id)
	if err != nil {
		return failOut(err, code, message)
	}
	args, err := decodeCallArgs(goString(argsJSON))
	if err != nil {
		return failOut(err, code, message)
	}
	var raw json.RawMessage
	if err := win.Call(goString(method), args, &raw); err != nil {
		return failOut(err, code, message)
	}
	dupJSON(raw, outJSON)
	return 0
}

//export brickly_window_send
func brickly_window_send(id C.uint64_t, name *C.char, payloadJSON *C.char, code **C.char, message **C.char) C.int {
	_, win, err := lookupWindow(id)
	if err != nil {
		return failOut(err, code, message)
	}
	payload, err := decodeJSONString(goString(payloadJSON))
	if err != nil {
		return failOut(err, code, message)
	}
	return failOut(win.Send(goString(name), payload), code, message)
}

//export brickly_window_expose
func brickly_window_expose(id C.uint64_t, name *C.char, fn C.brickly_window_rpc_fn, user unsafe.Pointer, code **C.char, message **C.char) C.int {
	_, win, err := lookupWindow(id)
	if err != nil {
		return failOut(err, code, message)
	}
	method := goString(name)
	if method == "" || fn == nil {
		return failOut(brickly.NewBppError("INVALID_INPUT", "expose 需要方法名和回调"), code, message)
	}
	err = win.Expose(map[string]brickly.WindowExposeHandler{
		method: func(payload any, _ brickly.WindowExposeSession) (any, error) {
			raw, encErr := encodeJSON(payload)
			if encErr != nil {
				raw = []byte("null")
			}
			cPayload := C.CString(string(raw))
			defer C.free(unsafe.Pointer(cPayload))
			var outJSON, errCode, errMsg *C.char
			rc := C.brickly_call_window_rpc(fn, cPayload, &outJSON, &errCode, &errMsg, user)
			defer brickly_string_free(outJSON)
			defer brickly_string_free(errCode)
			defer brickly_string_free(errMsg)
			if rc != 0 {
				c, m := goString(errCode), goString(errMsg)
				if c == "" {
					c = "INTERNAL"
				}
				return nil, brickly.NewBppError(c, m)
			}
			return decodeJSONString(goString(outJSON))
		},
	})
	return failOut(err, code, message)
}

//export brickly_window_on
func brickly_window_on(id C.uint64_t, event *C.char, fn C.brickly_json_fn, user unsafe.Pointer, sub *C.uint64_t, code **C.char, message **C.char) C.int {
	s, win, err := lookupWindow(id)
	if err != nil {
		return failOut(err, code, message)
	}
	if fn == nil || sub == nil {
		return failOut(brickly.NewBppError("INVALID_INPUT", "window on 需要回调"), code, message)
	}
	unsub := win.On(goString(event), func(payload map[string]any) {
		raw, encErr := encodeJSON(payload)
		if encErr != nil {
			raw = []byte("{}")
		}
		cstr := C.CString(string(raw))
		defer C.free(unsafe.Pointer(cstr))
		defer func() { _ = recover() }()
		C.brickly_call_json(fn, cstr, user)
	})
	*sub = C.uint64_t(alloc(&slot{rt: s.rt, unsub: unsub}))
	return 0
}

func callUiCreateWindow(id uint64, url, opts string) (handle uint64, code, message string, rc int) {
	cURL := C.CString(url)
	cOpts := C.CString(opts)
	defer C.free(unsafe.Pointer(cURL))
	defer C.free(unsafe.Pointer(cOpts))
	var out C.uint64_t
	var cCode, cMsg *C.char
	rc = int(brickly_ui_create_window(C.uint64_t(id), cURL, cOpts, &out, &cCode, &cMsg))
	code, message = goString(cCode), goString(cMsg)
	brickly_string_free(cCode)
	brickly_string_free(cMsg)
	return uint64(out), code, message, rc
}

func windowID(id uint64) int64 {
	return int64(brickly_window_id(C.uint64_t(id)))
}

func windowIsClosed(id uint64) bool {
	return brickly_window_is_closed(C.uint64_t(id)) == 1
}

func callWindowClose(id uint64) (out, code, message string, rc int) {
	var cOut, cCode, cMsg *C.char
	rc = int(brickly_window_close(C.uint64_t(id), &cOut, &cCode, &cMsg))
	out, code, message = goString(cOut), goString(cCode), goString(cMsg)
	brickly_string_free(cOut)
	brickly_string_free(cCode)
	brickly_string_free(cMsg)
	return
}

func callWindowCall(id uint64, method, args string) (out, code, message string, rc int) {
	cMethod := C.CString(method)
	cArgs := C.CString(args)
	defer C.free(unsafe.Pointer(cMethod))
	defer C.free(unsafe.Pointer(cArgs))
	var cOut, cCode, cMsg *C.char
	rc = int(brickly_window_call(C.uint64_t(id), cMethod, cArgs, &cOut, &cCode, &cMsg))
	out, code, message = goString(cOut), goString(cCode), goString(cMsg)
	brickly_string_free(cOut)
	brickly_string_free(cCode)
	brickly_string_free(cMsg)
	return
}

func callWindowSend(id uint64, name, payload string) (code, message string, rc int) {
	cName := C.CString(name)
	cPayload := C.CString(payload)
	defer C.free(unsafe.Pointer(cName))
	defer C.free(unsafe.Pointer(cPayload))
	var cCode, cMsg *C.char
	rc = int(brickly_window_send(C.uint64_t(id), cName, cPayload, &cCode, &cMsg))
	code, message = goString(cCode), goString(cMsg)
	brickly_string_free(cCode)
	brickly_string_free(cMsg)
	return
}

func windowRelease(id uint64) {
	brickly_window_release(C.uint64_t(id))
}
