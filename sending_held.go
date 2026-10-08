package meowcaller

import (
	"errors"
	"sync/atomic"
)

// errSendingHeld is a send's error while the call's sending is held.
var errSendingHeld = errors.New("meowcaller: the call's sending is held")

// WithSendingHeld makes RunMedia hold the call's sending until MediaCall.StartSending.
func WithSendingHeld() Option {
	// Source of truth: https://github.com/Hellysonrp/meowcaller/blob/3d49711d486ddb54af1c918043f93940061787bb/datasheets/sending-held.md#L20-L22
	return func(c *config) { c.sendingHeld = true }
}

// sendingIsHeld reports whether held holds the sending; a nil held never does.
func sendingIsHeld(held *atomic.Bool) bool {
	// Source of truth: https://github.com/Hellysonrp/meowcaller/blob/3d49711d486ddb54af1c918043f93940061787bb/datasheets/sending-held.md#L20
	return held != nil && held.Load()
}
