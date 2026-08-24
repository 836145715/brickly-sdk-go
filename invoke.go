package brickly

// InvokeOption 配置跨 Brick 调用。默认使用目标 Brick 的默认 Profile。
type InvokeOption func(*invokeOptions)

type invokeOptions struct {
	profileID       string
	parentRequestID string
	trace           *TraceContext
}

// WithProfileID 指定目标 Brick 的 Profile ID。
func WithProfileID(profileID string) InvokeOption {
	return func(opts *invokeOptions) {
		opts.profileID = profileID
	}
}

func collectInvokeOptions(opts []InvokeOption) invokeOptions {
	options := invokeOptions{}
	for _, opt := range opts {
		if opt != nil {
			opt(&options)
		}
	}
	return options
}

func parentInvocationRequired(message string) *BppError {
	return NewBppError("PARENT_INVOCATION_REQUIRED", message)
}

func (p *Runtime) invokePrepared(ref BrickRef, commandID string, input any, into any, options invokeOptions) error {
	if err := validateBrickRef(ref); err != nil {
		return err
	}
	prepared, err := prepareResourceValue(input)
	if err != nil {
		return err
	}
	return p.connectorInvoke(ref.BrickID, commandID, prepared, options.parentRequestID, into)
}

func validateBrickRef(ref BrickRef) error {
	if ref.BrickID == "" {
		return NewBppError("INVALID_INPUT", "BrickRef.brickId is required")
	}
	if ref.Version == "" {
		return NewBppError("INVALID_INPUT", "BrickRef.version is required")
	}
	switch ref.Origin {
	case BrickOriginInstalled, BrickOriginDevelopment, BrickOriginReview:
		return nil
	default:
		return NewBppError("INVALID_INPUT", "BrickRef.origin is invalid")
	}
}
