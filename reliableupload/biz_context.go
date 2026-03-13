package reliableupload

import "context"

type bizTriggerContextKey struct{}

// WithBizTrigger injects business trigger context for DataSource implementation.
func WithBizTrigger(ctx context.Context, trigger BizTrigger) context.Context {
	return context.WithValue(ctx, bizTriggerContextKey{}, trigger)
}

// BizTriggerFromContext reads business trigger context.
func BizTriggerFromContext(ctx context.Context) (BizTrigger, bool) {
	v := ctx.Value(bizTriggerContextKey{})
	if v == nil {
		return BizTrigger{}, false
	}
	trigger, ok := v.(BizTrigger)
	return trigger, ok
}
