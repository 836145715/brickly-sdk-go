package grpc

import (
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

// Host 客户端 `grpc.keepalive_time_ms` 为 30s。
// grpc-go 默认 EnforcementPolicy.MinTime 是 5 分钟，会把空闲 interact 打成
// GOAWAY / RESOURCE_EXHAUSTED。Runtime 必须允许 Host 的间隔，不得收得更严。
const (
	HostKeepaliveTime       = 30 * time.Second
	RuntimeKeepaliveMinTime = 10 * time.Second
)

func runtimeKeepaliveEnforcement() keepalive.EnforcementPolicy {
	return keepalive.EnforcementPolicy{
		MinTime:             RuntimeKeepaliveMinTime,
		PermitWithoutStream: true,
	}
}

func runtimeKeepaliveServerOption() grpc.ServerOption {
	return grpc.KeepaliveEnforcementPolicy(runtimeKeepaliveEnforcement())
}
