package meowcaller

import "go.mau.fi/whatsmeow/types"

func (e *engine) recordPreAccept(callID string, from types.JID) {
	// Source of truth: https://github.com/Hellysonrp/meowcaller/blob/deca4540fc12e4d012b1ef8cbfc66e9a7b4787b9/datasheets/outgoing-reject.md#L29
	if from.IsEmpty() {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	m := e.calls[callID]
	if m == nil {
		return
	}
	if m.preAccepted == nil {
		m.preAccepted = make(map[types.JID]struct{})
	}
	m.preAccepted[from] = struct{}{}
}

func (e *engine) ignoresSecondaryReject(callID string, from types.JID) bool {
	// Source of truth: https://github.com/Hellysonrp/meowcaller/blob/deca4540fc12e4d012b1ef8cbfc66e9a7b4787b9/datasheets/outgoing-reject.md#L27-L30
	if from.Device == 0 {
		return false
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	m := e.calls[callID]
	if m == nil || m.direction != CallDirectionOutgoing || m.group {
		return false
	}
	_, preAccepted := m.preAccepted[from]
	return !preAccepted
}
