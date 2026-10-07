package meowcaller

import "sync/atomic"

// WithSendingHeld makes RunMedia hold the call's sending until MediaCall.StartSending.
func WithSendingHeld() Option {
	// Source of truth: https://github.com/Hellysonrp/meowcaller/blob/3d49711d486ddb54af1c918043f93940061787bb/datasheets/sending-held.md#L20-L22
	// TODO
	// agent suggestion: return an Option that sets config.sendingHeld; RunMedia gives the call a sendHeld flag storing true when it is set; NewClient ignores it.
	// human input:
	return func(*config) {}
}

// sendingIsHeld reports whether held holds the sending; a nil held never does.
func sendingIsHeld(held *atomic.Bool) bool {
	// Source of truth: https://github.com/Hellysonrp/meowcaller/blob/3d49711d486ddb54af1c918043f93940061787bb/datasheets/sending-held.md#L20
	// TODO
	// agent suggestion: return held != nil && held.Load().
	// human input:
	return false
}
