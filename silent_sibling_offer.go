package meowcaller

import (
	"github.com/purpshell/meowcaller/signaling"
	"go.mau.fi/whatsmeow/types"
)

// ownAccountConnected reports whether participants shows one of self's users in state
// "connected".
func ownAccountConnected(participants []signaling.GroupCallParticipant, self []types.JID) bool {
	// Source of truth: https://github.com/Hellysonrp/meowcaller/blob/8bbcbde18ee9f8125c627fb96f7e451dfb4c69d7/datasheets/silent-sibling-offer.md#L35-L37
	// TODO
	// agent suggestion: collect self's non-empty JIDs as ToNonAD; true when a participant in state "connected" has a non-empty jid or user_pn whose ToNonAD is among them.
	// human input:
	return false
}
