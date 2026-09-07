#ifndef BRICKLY_CAPI_CALLBACK_H
#define BRICKLY_CAPI_CALLBACK_H

#include <stdint.h>

/* 仅 capi 内部 trampoline；产品头文件是 brickly-sdk-cpp/include/brickly.h */

typedef void (*brickly_command_fn)(uint64_t ctx, const char *input_json, void *user);
typedef void (*brickly_hook_fn)(void *user);
typedef void (*brickly_json_fn)(const char *json, void *user);
typedef void (*brickly_event_fn)(const char *event, const char *payload_json, const char *envelope_json, void *user);
/* 返回 0 成功：*out_json 由回调 malloc，Go 侧 brickly_string_free */
typedef int (*brickly_window_rpc_fn)(const char *payload_json, char **out_json, char **code, char **message, void *user);

static void brickly_call_command(brickly_command_fn fn, uint64_t ctx, const char *input_json, void *user) {
	if (fn) {
		fn(ctx, input_json, user);
	}
}

static void brickly_call_hook(brickly_hook_fn fn, void *user) {
	if (fn) {
		fn(user);
	}
}

static void brickly_call_json(brickly_json_fn fn, const char *json, void *user) {
	if (fn) {
		fn(json, user);
	}
}

static void brickly_call_event(brickly_event_fn fn, const char *event, const char *payload_json, const char *envelope_json, void *user) {
	if (fn) {
		fn(event, payload_json, envelope_json, user);
	}
}

static int brickly_call_window_rpc(brickly_window_rpc_fn fn, const char *payload_json, char **out_json, char **code, char **message, void *user) {
	if (!fn) {
		return 1;
	}
	return fn(payload_json, out_json, code, message, user);
}

#endif
