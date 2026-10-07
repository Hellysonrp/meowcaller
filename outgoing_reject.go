package meowcaller

import "go.mau.fi/whatsmeow/types"

func (e *engine) recordPreAccept(callID string, from types.JID) {
	// Source of truth: https://github.com/Hellysonrp/meowcaller/blob/deca4540fc12e4d012b1ef8cbfc66e9a7b4787b9/datasheets/outgoing-reject.md#L29
	// TODO
	// agent suggestion: ignore an empty JID; under e.mu, add from to the call's preAccepted set, creating it on first use.
	// human input:
}

func (e *engine) ignoresSecondaryReject(callID string, from types.JID) bool {
	// Source of truth: https://github.com/Hellysonrp/meowcaller/blob/deca4540fc12e4d012b1ef8cbfc66e9a7b4787b9/datasheets/outgoing-reject.md#L27-L30
	// TODO
	// agent suggestion: false for device 0; under e.mu, true only for a live outgoing 1:1 call whose preAccepted set lacks from.
	// human input:
	return false
}
