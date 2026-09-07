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

func systemAPI(s *slot) *brickly.SystemAPI {
	if s == nil {
		return nil
	}
	if s.ctx != nil {
		return s.ctx.System()
	}
	if s.rt != nil {
		return s.rt.System
	}
	return nil
}

func clipboardAPI(s *slot) *brickly.ClipboardAPI {
	if s == nil {
		return nil
	}
	if s.ctx != nil {
		return s.ctx.Platform().Clipboard
	}
	if s.rt != nil && s.rt.Platform != nil {
		return s.rt.Platform.Clipboard
	}
	return nil
}

func requireClient(s *slot, alias string) (*brickly.DependencyClient, error) {
	if s == nil {
		return nil, invalidHandle()
	}
	if s.ctx != nil {
		return s.ctx.Dependencies().Require(alias)
	}
	if s.rt != nil {
		return s.rt.Dependencies.Require(alias)
	}
	return nil, invalidHandle()
}

//export brickly_require_invoke
func brickly_require_invoke(id C.uint64_t, alias *C.char, commandID *C.char, inputJSON *C.char, outJSON **C.char, code **C.char, message **C.char) C.int {
	s := lookup(uint64(id))
	client, err := requireClient(s, goString(alias))
	if err != nil {
		return failOut(err, code, message)
	}
	input, err := decodeJSONString(goString(inputJSON))
	if err != nil {
		return failOut(err, code, message)
	}
	var out any
	if err := client.Invoke(goString(commandID), input, &out); err != nil {
		return failOut(err, code, message)
	}
	if err := writeJSONValue(out, outJSON); err != nil {
		return failOut(err, code, message)
	}
	return 0
}

//export brickly_events_on
func brickly_events_on(id C.uint64_t, event *C.char, fn C.brickly_event_fn, user unsafe.Pointer, sub *C.uint64_t, code **C.char, message **C.char) C.int {
	s := lookup(uint64(id))
	if s == nil || s.rt == nil || fn == nil || sub == nil {
		return failOut(invalidHandle(), code, message)
	}
	name := goString(event)
	var unsub func()
	var panicErr error
	func() {
		defer func() {
			panicErr = recoverError(recover())
		}()
		unsub = s.rt.Events.On(name, func(payload any, env brickly.EventEnvelope) {
			raw, err := encodeJSON(payload)
			if err != nil {
				raw = []byte("null")
			}
			envRaw, err := encodeJSON(map[string]any{
				"event":       env.Event,
				"source":      env.Source,
				"publishedAt": env.PublishedAt,
			})
			if err != nil {
				envRaw = []byte("{}")
			}
			cEvent := C.CString(env.Event)
			cPayload := C.CString(string(raw))
			cEnv := C.CString(string(envRaw))
			defer C.free(unsafe.Pointer(cEvent))
			defer C.free(unsafe.Pointer(cPayload))
			defer C.free(unsafe.Pointer(cEnv))
			defer func() { _ = recover() }()
			C.brickly_call_event(fn, cEvent, cPayload, cEnv, user)
		})
	}()
	if panicErr != nil {
		return failOut(panicErr, code, message)
	}
	*sub = C.uint64_t(alloc(&slot{rt: s.rt, unsub: unsub}))
	return 0
}

//export brickly_events_off
func brickly_events_off(id C.uint64_t) {
	s := lookup(uint64(id))
	if s == nil {
		return
	}
	if s.unsub != nil {
		s.unsub()
	}
	drop(uint64(id))
}

//export brickly_events_publish
func brickly_events_publish(id C.uint64_t, event *C.char, payloadJSON *C.char, code **C.char, message **C.char) C.int {
	s := lookup(uint64(id))
	if s == nil || s.rt == nil {
		return failOut(invalidHandle(), code, message)
	}
	payload, err := decodeJSONString(goString(payloadJSON))
	if err != nil {
		return failOut(err, code, message)
	}
	return failOut(s.rt.Events.Publish(goString(event), payload), code, message)
}

//export brickly_clipboard_read
func brickly_clipboard_read(id C.uint64_t, outJSON **C.char, code **C.char, message **C.char) C.int {
	clip := clipboardAPI(lookup(uint64(id)))
	if clip == nil {
		return failOut(invalidHandle(), code, message)
	}
	result, err := clip.ReadContent()
	if err != nil {
		return failOut(err, code, message)
	}
	return failOut(writeJSONValue(result, outJSON), code, message)
}

//export brickly_clipboard_set
func brickly_clipboard_set(id C.uint64_t, contentJSON *C.char, outJSON **C.char, code **C.char, message **C.char) C.int {
	clip := clipboardAPI(lookup(uint64(id)))
	if clip == nil {
		return failOut(invalidHandle(), code, message)
	}
	value, err := decodeJSONString(goString(contentJSON))
	if err != nil {
		return failOut(err, code, message)
	}
	content, _ := value.(map[string]any)
	result, err := clip.SetContent(content)
	if err != nil {
		return failOut(err, code, message)
	}
	return failOut(writeJSONValue(result, outJSON), code, message)
}

//export brickly_system_get_path
func brickly_system_get_path(id C.uint64_t, name *C.char, out **C.char, code **C.char, message **C.char) C.int {
	sys := systemAPI(lookup(uint64(id)))
	if sys == nil {
		return failOut(invalidHandle(), code, message)
	}
	path, err := sys.GetPath(brickly.SystemPathName(goString(name)))
	if err != nil {
		return failOut(err, code, message)
	}
	if out != nil {
		*out = C.CString(path)
	}
	return 0
}

//export brickly_system_is_windows
func brickly_system_is_windows(id C.uint64_t, out *C.int, code **C.char, message **C.char) C.int {
	sys := systemAPI(lookup(uint64(id)))
	if sys == nil {
		return failOut(invalidHandle(), code, message)
	}
	ok, err := sys.IsWindows()
	if err != nil {
		return failOut(err, code, message)
	}
	if out != nil {
		if ok {
			*out = 1
		} else {
			*out = 0
		}
	}
	return 0
}

//export brickly_system_get_app_name
func brickly_system_get_app_name(id C.uint64_t, out **C.char, code **C.char, message **C.char) C.int {
	sys := systemAPI(lookup(uint64(id)))
	if sys == nil {
		return failOut(invalidHandle(), code, message)
	}
	name, err := sys.GetAppName()
	if err != nil {
		return failOut(err, code, message)
	}
	if out != nil {
		*out = C.CString(name)
	}
	return 0
}

//export brickly_resource_create
func brickly_resource_create(id C.uint64_t, data unsafe.Pointer, length C.size_t, mime *C.char, name *C.char, out *C.uint64_t, code **C.char, message **C.char) C.int {
	s := lookup(uint64(id))
	if s == nil || out == nil {
		return failOut(invalidHandle(), code, message)
	}
	var bytes []byte
	if data != nil && length > 0 {
		bytes = C.GoBytes(data, C.int(length))
	}
	opts := &brickly.ResourceCreateOptions{MimeType: goString(mime), Name: goString(name)}
	var handle *brickly.ResourceHandle
	var err error
	if s.ctx != nil {
		handle, err = s.ctx.CreateResource(bytes, opts)
	} else if s.rt != nil {
		handle, err = s.rt.CreateResource(bytes, opts)
	} else {
		err = invalidHandle()
	}
	if err != nil {
		return failOut(err, code, message)
	}
	*out = C.uint64_t(alloc(&slot{rt: s.rt, res: handle}))
	return 0
}

//export brickly_resource_open
func brickly_resource_open(id C.uint64_t, refJSON *C.char, out *C.uint64_t, code **C.char, message **C.char) C.int {
	s := lookup(uint64(id))
	if s == nil || s.rt == nil || out == nil {
		return failOut(invalidHandle(), code, message)
	}
	var ref brickly.ResourceRef
	if err := json.Unmarshal([]byte(goString(refJSON)), &ref); err != nil {
		return failOut(brickly.NewBppError("INVALID_RESOURCE_REF", "ResourceRef JSON 无效"), code, message)
	}
	handle, err := s.rt.OpenResource(ref)
	if err != nil {
		return failOut(err, code, message)
	}
	*out = C.uint64_t(alloc(&slot{rt: s.rt, res: handle}))
	return 0
}

func lookupResource(id C.uint64_t) (*brickly.ResourceHandle, error) {
	s := lookup(uint64(id))
	if s == nil || s.res == nil {
		return nil, invalidHandle()
	}
	return s.res, nil
}

//export brickly_resource_ref_json
func brickly_resource_ref_json(id C.uint64_t) *C.char {
	handle, err := lookupResource(id)
	if err != nil {
		return C.CString("null")
	}
	raw, err := encodeJSON(handle.Ref)
	if err != nil {
		return C.CString("null")
	}
	return C.CString(string(raw))
}

//export brickly_resource_text
func brickly_resource_text(id C.uint64_t, out **C.char, code **C.char, message **C.char) C.int {
	handle, err := lookupResource(id)
	if err != nil {
		return failOut(err, code, message)
	}
	text, err := handle.Text()
	if err != nil {
		return failOut(err, code, message)
	}
	if out != nil {
		*out = C.CString(text)
	}
	return 0
}

//export brickly_resource_bytes
func brickly_resource_bytes(id C.uint64_t, data *unsafe.Pointer, length *C.size_t, code **C.char, message **C.char) C.int {
	handle, err := lookupResource(id)
	if err != nil {
		return failOut(err, code, message)
	}
	raw, err := handle.Bytes()
	if err != nil {
		return failOut(err, code, message)
	}
	ptr, n := cBytes(raw)
	if data != nil {
		*data = ptr
	} else if ptr != nil {
		C.free(ptr)
	}
	if length != nil {
		*length = n
	}
	return 0
}

//export brickly_resource_save_to
func brickly_resource_save_to(id C.uint64_t, path *C.char, code **C.char, message **C.char) C.int {
	handle, err := lookupResource(id)
	if err != nil {
		return failOut(err, code, message)
	}
	return failOut(handle.SaveTo(goString(path)), code, message)
}

//export brickly_resource_close
func brickly_resource_close(id C.uint64_t, code **C.char, message **C.char) C.int {
	handle, err := lookupResource(id)
	if err != nil {
		return failOut(err, code, message)
	}
	closeErr := handle.Close()
	drop(uint64(id))
	return failOut(closeErr, code, message)
}

//export brickly_resource_revoke
func brickly_resource_revoke(id C.uint64_t, code **C.char, message **C.char) C.int {
	handle, err := lookupResource(id)
	if err != nil {
		return failOut(err, code, message)
	}
	revokeErr := handle.Revoke()
	drop(uint64(id))
	return failOut(revokeErr, code, message)
}
