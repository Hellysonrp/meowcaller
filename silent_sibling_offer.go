package meowcaller

import (
	"github.com/purpshell/meowcaller/signaling"
	"go.mau.fi/whatsmeow/types"
)

// ownAccountConnected reports whether participants shows one of self's users in state
// "connected".
func ownAccountConnected(participants []signaling.GroupCallParticipant, self []types.JID) bool {
	// Source of truth: https://github.com/Hellysonrp/meowcaller/blob/8bbcbde18ee9f8125c627fb96f7e451dfb4c69d7/datasheets/silent-sibling-offer.md#L35-L37
	own := make(map[types.JID]struct{}, len(self))
	for _, jid := range self {
		if !jid.IsEmpty() {
			own[jid.ToNonAD()] = struct{}{}
		}
	}
	for _, participant := range participants {
		if participant.State != "connected" {
			continue
		}
		for _, jid := range []types.JID{participant.JID, participant.PN} {
			if _, ok := own[jid.ToNonAD()]; ok {
				return true
			}
		}
	}
	return false
}
