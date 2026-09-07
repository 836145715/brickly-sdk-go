package main

/*
#include "callback.h"
#include <stdlib.h>
*/
import "C"
import "unsafe"

func goString(s *C.char) string {
	if s == nil {
		return ""
	}
	return C.GoString(s)
}

func cString(s string) *C.char {
	return C.CString(s)
}

func failOut(err error, code **C.char, message **C.char) C.int {
	if err == nil {
		return 0
	}
	c, m := bppCodeMessage(err)
	if code != nil {
		*code = C.CString(c)
	}
	if message != nil {
		*message = C.CString(m)
	}
	return 1
}

func dupJSON(raw []byte, out **C.char) {
	if out == nil {
		return
	}
	if len(raw) == 0 {
		*out = C.CString("null")
		return
	}
	*out = C.CString(string(raw))
}

func writeJSONValue(value any, out **C.char) error {
	raw, err := encodeJSON(value)
	if err != nil {
		return err
	}
	dupJSON(raw, out)
	return nil
}

func cBytes(data []byte) (unsafe.Pointer, C.size_t) {
	if len(data) == 0 {
		return nil, 0
	}
	return C.CBytes(data), C.size_t(len(data))
}
