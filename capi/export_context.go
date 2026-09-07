package main

/*
#include "callback.h"
#include <stdlib.h>
*/
import "C"
import (
	"unsafe"

	brickly "github.com/836145715/brickly-sdk-go"
)

func lookupCtx(id C.uint64_t) (*slot, *brickly.CommandContext, error) {
	s := lookup(uint64(id))
	if s == nil || s.ctx == nil {
		return nil, nil, invalidHandle()
	}
	return s, s.ctx, nil
}

//export brickly_ctx_request_id
func brickly_ctx_request_id(id C.uint64_t) *C.char {
	_, ctx, err := lookupCtx(id)
	if err != nil {
		return C.CString("")
	}
	return C.CString(ctx.RequestID)
}

//export brickly_ctx_command_id
func brickly_ctx_command_id(id C.uint64_t) *C.char {
	_, ctx, err := lookupCtx(id)
	if err != nil {
		return C.CString("")
	}
	return C.CString(ctx.CommandID)
}

//export brickly_ctx_is_cancelled
func brickly_ctx_is_cancelled(id C.uint64_t) C.int {
	_, ctx, err := lookupCtx(id)
	if err != nil {
		return -1
	}
	if ctx.IsCancelled() {
		return 1
	}
	return 0
}

//export brickly_ctx_send
func brickly_ctx_send(id C.uint64_t, eventJSON *C.char, code **C.char, message **C.char) C.int {
	_, ctx, err := lookupCtx(id)
	if err != nil {
		return failOut(err, code, message)
	}
	event, err := decodeJSONString(goString(eventJSON))
	if err != nil {
		return failOut(err, code, message)
	}
	return failOut(ctx.Send(event), code, message)
}

//export brickly_ctx_on_event
func brickly_ctx_on_event(id C.uint64_t, fn C.brickly_json_fn, user unsafe.Pointer, code **C.char, message **C.char) C.int {
	s, ctx, err := lookupCtx(id)
	if err != nil {
		return failOut(err, code, message)
	}
	if fn == nil {
		return failOut(brickly.NewBppError("INVALID_INPUT", "onEvent handler 不能为空"), code, message)
	}
	live := s.live
	err = ctx.OnEvent(func(event any) {
		if live != nil && !live.ok() {
			return
		}
		raw, encErr := encodeJSON(event)
		if encErr != nil {
			return
		}
		cstr := C.CString(string(raw))
		defer C.free(unsafe.Pointer(cstr))
		defer func() { _ = recover() }()
		C.brickly_call_json(fn, cstr, user)
	})
	return failOut(err, code, message)
}

//export brickly_ctx_wait_closed
func brickly_ctx_wait_closed(id C.uint64_t) {
	_, ctx, err := lookupCtx(id)
	if err != nil {
		return
	}
	<-ctx.Closed()
}

//export brickly_ctx_reply
func brickly_ctx_reply(id C.uint64_t, jsonText *C.char) {
	s, _, err := lookupCtx(id)
	if err != nil {
		return
	}
	s.result.reply([]byte(goString(jsonText)))
}

//export brickly_ctx_fail
func brickly_ctx_fail(id C.uint64_t, code *C.char, message *C.char) {
	s, _, err := lookupCtx(id)
	if err != nil {
		return
	}
	c := goString(code)
	if c == "" {
		c = "INTERNAL"
	}
	s.result.fail(brickly.NewBppError(c, goString(message)))
}

//export brickly_ctx_log
func brickly_ctx_log(id C.uint64_t, level *C.char, message *C.char, fieldsJSON *C.char) {
	s := lookup(uint64(id))
	if s == nil {
		return
	}
	logWith(s, goString(level), goString(message), parseFields(goString(fieldsJSON)), nil)
}
