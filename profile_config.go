package brickly

import (
	"encoding/json"
	"os"
)

const (
	ProfileIDEnv     = "BRICKLY_PROFILE_ID"
	ProfileConfigEnv = "BRICKLY_PROFILE_CONFIG"
)

// ReadInjectedProfileConfig 读 Host spawn 注入的 Profile 配置快照。非法 JSON 忽略。
func ReadInjectedProfileConfig() map[string]any {
	return readInjectedProfileConfig(os.Getenv(ProfileConfigEnv))
}

func readInjectedProfileConfig(raw string) map[string]any {
	if raw == "" {
		return map[string]any{}
	}
	var parsed any
	if err := json.Unmarshal([]byte(raw), &parsed); err != nil {
		return map[string]any{}
	}
	object, ok := parsed.(map[string]any)
	if !ok {
		return map[string]any{}
	}
	return object
}
