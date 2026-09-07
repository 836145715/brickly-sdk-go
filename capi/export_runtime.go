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

var (
	staticVersion  = C.CString(brickly.SdkVersion)
	staticProtocol = C.CString(brickly.ProtocolVersion)
)

//export brickly_version
func brickly_version() *C.char { return staticVersion }

//export brickly_protocol_version
func brickly_protocol_version() *C.char { return staticProtocol }

//export brickly_string_free
func brickly_string_free(p *C.char) {
	if p != nil {
		C.free(unsafe.Pointer(p))
	}
}

//export brickly_bytes_free
func brickly_bytes_free(p unsafe.Pointer) {
	if p != nil {
		C.free(p)
	}
}

//export brickly_new
func brickly_new() C.uint64_t {
	rt := brickly.New()
	return C.uint64_t(alloc(&slot{rt: rt}))
}

//export brickly_free
func brickly_free(id C.uint64_t) {
	s := lookup(uint64(id))
	if s == nil || s.rt == nil || s.ctx != nil {
		drop(uint64(id))
		return
	}
	drop(uint64(id))
}

//export brickly_on_command
func brickly_on_command(id C.uint64_t, commandID *C.char, fn C.brickly_command_fn, user unsafe.Pointer) C.int {
	s := lookup(uint64(id))
	if s == nil || s.rt == nil {
		return 1
	}
	name := goString(commandID)
	if name == "" || fn == nil {
		return 1
	}
	rt := s.rt
	rt.OnCommand(name, func(ctx *brickly.CommandContext, input json.RawMessage) (any, error) {
		result := &commandResult{}
		live := newLiveFlag()
		ctxID := alloc(&slot{rt: rt, ctx: ctx, result: result, live: live})
		defer func() {
			live.set(false)
			drop(ctxID)
		}()
		cinput := C.CString(string(input))
		defer C.free(unsafe.Pointer(cinput))
		func() {
			defer func() {
				if rec := recover(); rec != nil {
					result.fail(recoverError(rec))
				}
			}()
			C.brickly_call_command(fn, C.uint64_t(ctxID), cinput, user)
		}()
		return result.take()
	})
	return 0
}

//export brickly_on_ready
func brickly_on_ready(id C.uint64_t, fn C.brickly_hook_fn, user unsafe.Pointer) C.int {
	s := lookup(uint64(id))
	if s == nil || s.rt == nil || fn == nil {
		return 1
	}
	s.rt.OnReady(func() error {
		defer func() { _ = recover() }()
		C.brickly_call_hook(fn, user)
		return nil
	})
	return 0
}

//export brickly_on_shutdown
func brickly_on_shutdown(id C.uint64_t, fn C.brickly_hook_fn, user unsafe.Pointer) C.int {
	s := lookup(uint64(id))
	if s == nil || s.rt == nil || fn == nil {
		return 1
	}
	s.rt.OnShutdown(func() error {
		defer func() { _ = recover() }()
		C.brickly_call_hook(fn, user)
		return nil
	})
	return 0
}

//export brickly_start
func brickly_start(id C.uint64_t) C.int {
	s := lookup(uint64(id))
	if s == nil || s.rt == nil {
		return 1
	}
	if err := s.rt.Start(); err != nil {
		return 1
	}
	return 0
}

//export brickly_log
func brickly_log(id C.uint64_t, level *C.char, message *C.char, fieldsJSON *C.char) {
	s := lookup(uint64(id))
	if s == nil {
		return
	}
	msg := goString(message)
	fields := parseFields(goString(fieldsJSON))
	logWith(s, goString(level), msg, fields, nil)
}

//export brickly_config_json
func brickly_config_json(id C.uint64_t) *C.char {
	s := lookup(uint64(id))
	if s == nil {
		return C.CString("{}")
	}
	cfg := map[string]any{}
	if s.ctx != nil {
		cfg = s.ctx.Config()
	} else if s.rt != nil {
		cfg = s.rt.Config
	}
	raw, err := encodeJSON(cfg)
	if err != nil {
		return C.CString("{}")
	}
	return C.CString(string(raw))
}

//export brickly_invoke
func brickly_invoke(id C.uint64_t, commandID *C.char, inputJSON *C.char, outJSON **C.char, code **C.char, message **C.char) C.int {
	s := lookup(uint64(id))
	if s == nil || s.rt == nil {
		return failOut(invalidHandle(), code, message)
	}
	input, err := decodeJSONString(goString(inputJSON))
	if err != nil {
		return failOut(err, code, message)
	}
	result, err := s.rt.Invoke(goString(commandID), input)
	if err != nil {
		return failOut(err, code, message)
	}
	if err := writeJSONValue(result, outJSON); err != nil {
		return failOut(err, code, message)
	}
	return 0
}

func logWith(s *slot, level, message string, fields map[string]any, err error) {
	if s == nil {
		return
	}
	if s.ctx != nil {
		switch level {
		case "debug":
			s.ctx.Debug(message, fields)
		case "warn":
			s.ctx.Warn(message, fields)
		case "error":
			s.ctx.Error(message, err, fields)
		default:
			s.ctx.Info(message, fields)
		}
		return
	}
	if s.rt == nil {
		return
	}
	switch level {
	case "debug":
		s.rt.Debug(message, fields)
	case "warn":
		s.rt.Warn(message, fields)
	case "error":
		s.rt.Error(message, err, fields)
	default:
		s.rt.Info(message, fields)
	}
}

func main() {}
