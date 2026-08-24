package grpc

import (
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
)

func StatusFromBrickCode(code, message string) error {
	mapped := codes.Internal
	switch code {
	case "INVALID_INPUT", "INVALID_RESOURCE_REF", "RESOURCE_OFFSET_MISMATCH":
		mapped = codes.InvalidArgument
	case "ACCESS_DENIED", "RESOURCE_ACCESS_DENIED":
		mapped = codes.PermissionDenied
	case "NOT_FOUND", "RESOURCE_NOT_FOUND":
		mapped = codes.NotFound
	case "CONFLICT":
		mapped = codes.Aborted
	case "LIMIT_EXCEEDED", "RESOURCE_QUOTA_EXCEEDED":
		mapped = codes.ResourceExhausted
	case "DEADLINE_EXCEEDED":
		mapped = codes.DeadlineExceeded
	case "CANCELLED":
		mapped = codes.Canceled
	case "OUTCOME_UNKNOWN":
		mapped = codes.Unavailable
	case "CALL_CYCLE_DETECTED", "REQUEST_HANDLER_UNAVAILABLE", "RESOURCE_EXPIRED", "RESOURCE_REVOKED":
		mapped = codes.FailedPrecondition
	case "RESOURCE_INTEGRITY_FAILED":
		mapped = codes.DataLoss
	case "PROTOCOL_VIOLATION", "INTERNAL":
		mapped = codes.Internal
	}
	if strings.Contains(message, "token") {
		message = strings.ReplaceAll(message, "token", "***")
	}
	st := status.New(mapped, message)
	detail := &BrickError{Code: code, Message: message}
	if code == "INVALID_INPUT" {
		if packed, err := anypb.New(&InvalidInputDetail{Field: "input", Reason: "invalid"}); err == nil {
			detail.Details = packed
		}
	}
	if withDetails, err := st.WithDetails(detail); err == nil {
		return withDetails.Err()
	}
	return st.Err()
}

func StatusFromError(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := status.FromError(err); ok {
		return err
	}
	if coded, ok := err.(interface{ BrickCode() string }); ok {
		return StatusFromBrickCode(coded.BrickCode(), err.Error())
	}
	if coded, ok := err.(interface{ Code() string }); ok {
		return StatusFromBrickCode(coded.Code(), err.Error())
	}
	return StatusFromBrickCode("INTERNAL", err.Error())
}
