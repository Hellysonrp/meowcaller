package meowcaller

import (
	"errors"
	"time"
)

// ErrRelayTimeout is the media loop's error when the relay sent nothing within the
// relay timeout.
var ErrRelayTimeout = errors.New("meowcaller: relay sent nothing within the relay timeout")

// WithRelayTimeout ends a call's media when the relay sends nothing for d. Zero or less
// disables it.
func WithRelayTimeout(d time.Duration) Option {
	// Source of truth: https://github.com/Hellysonrp/meowcaller/blob/c94a2d54a5179636eff02b57575f2ee8e8372cf6/datasheets/relay-timeout.md#L17-L21
	return func(c *config) { c.relayTimeout = d }
}
