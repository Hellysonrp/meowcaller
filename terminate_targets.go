package meowcaller

import (
	"slices"

	"go.mau.fi/whatsmeow/types"
)

// terminateTargets returns, from one snapshot under the engine lock, the JIDs a hangup of
// c sends a terminate to and the call-creator they carry.
func (e *engine) terminateTargets(c *Call) ([]types.JID, types.JID) {
	// Source of truth: https://github.com/oxidezap/whatsapp-rust/blob/e634d245f71d66e2ae55f79ef714c4ca01615ed8/src/voip/facade.rs#L4113-L4139
	peer := c.Peer()
	e.mu.Lock()
	defer e.mu.Unlock()
	m := e.calls[c.id]
	if m == nil {
		return []types.JID{peer}, peer
	}
	if m.group || m.direction != CallDirectionOutgoing || m.peerAnswered || len(m.ringDevices) == 0 {
		return []types.JID{m.from}, m.creator
	}
	return slices.Clone(m.ringDevices), m.creator
}
