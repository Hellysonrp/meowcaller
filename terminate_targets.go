package meowcaller

import "go.mau.fi/whatsmeow/types"

// terminateTargets returns, from one snapshot under the engine lock, the JIDs a hangup of
// c sends a terminate to and the call-creator they carry.
func (e *engine) terminateTargets(c *Call) ([]types.JID, types.JID) {
	// Source of truth: https://github.com/oxidezap/whatsapp-rust/blob/e634d245f71d66e2ae55f79ef714c4ca01615ed8/src/voip/facade.rs#L4113-L4139
	// TODO
	// agent suggestion: under e.mu, with no entry return [c.peer] and c.peer; for a group
	// call, an incoming call, peerAnswered or an empty ringDevices return [m.from] and
	// m.creator; otherwise a copy of ringDevices and m.creator.
	// human input:
	return nil, types.EmptyJID
}
