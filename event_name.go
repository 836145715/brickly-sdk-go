package brickly

import "regexp"

var windowHostEventTopics = []string{
	"window.notify",
	"window.request",
	"window.request.cancel",
	"window.closed",
	"window.focus",
	"window.blur",
	"window.resize",
	"window.move",
	"window.show",
	"window.hide",
}

var publicEventName = regexp.MustCompile(`^[A-Za-z0-9_.-]+:[A-Za-z0-9_.:-]+$`)

const publicEventNameHint = "公共事件名必须是「命名空间:主题」，例如 clipboard:new-content、my-brick:tick。只允许字母、数字、_ . - :，中间必须有冒号。"

func requirePublicEventName(event string) error {
	if publicEventName.MatchString(event) {
		return nil
	}
	return NewBppError("INVALID_INPUT", "无效的事件名："+event+"。"+publicEventNameHint)
}
