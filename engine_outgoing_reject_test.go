package meowcaller

import (
	"testing"

	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

func skipOutgoingRejectStub(t *testing.T) {
	t.Helper()
	t.Skip("blocked: engine/outgoing-reject is a stub; enable when implemented")
}

func peerDevice(device uint16) types.JID {
	jid := peerJID()
	jid.Device = device
	return jid
}

func preacceptFrom(eng *engine, from types.JID) {
	eng.onPreAccept(&events.CallPreAccept{BasicCallMeta: types.BasicCallMeta{CallID: "CID", From: from}})
}

func rejectFrom(eng *engine, from types.JID) {
	eng.onReject(&events.CallReject{BasicCallMeta: types.BasicCallMeta{CallID: "CID", From: from}})
}

func TestSecondaryDeviceRejectBeforePreacceptIsIgnored(t *testing.T) {
	skipOutgoingRejectStub(t)
	eng, call := testEngineWithOutgoingCall()

	rejectFrom(eng, peerDevice(22))

	if call.State() == CallPhaseEnded || eng.lookup("CID") == nil {
		t.Fatal("a secondary device's reject ended the call before anyone rang")
	}
}

func TestSecondaryDeviceRejectAfterPhonePreacceptIsIgnored(t *testing.T) {
	skipOutgoingRejectStub(t)
	eng, call := testEngineWithOutgoingCall()

	preacceptFrom(eng, peerDevice(0))
	rejectFrom(eng, peerDevice(22))

	if call.State() == CallPhaseEnded || eng.lookup("CID") == nil {
		t.Fatal("a secondary device's reject ended the call while the phone rang")
	}
}

func TestPrimaryDeviceRejectEndsOutgoingCall(t *testing.T) {
	skipOutgoingRejectStub(t)
	eng, call := testEngineWithOutgoingCall()

	rejectFrom(eng, peerDevice(0))

	if call.State() != CallPhaseEnded {
		t.Fatalf("phase = %d, want Ended", call.State())
	}
}

func TestPreacceptedDeviceRejectEndsOutgoingCall(t *testing.T) {
	skipOutgoingRejectStub(t)
	eng, call := testEngineWithOutgoingCall()

	preacceptFrom(eng, peerDevice(0))
	preacceptFrom(eng, peerDevice(5))
	rejectFrom(eng, peerDevice(5))

	if call.State() != CallPhaseEnded {
		t.Fatalf("phase = %d, want Ended", call.State())
	}
}

func TestIncomingCallRejectFromSecondaryDeviceEndsCall(t *testing.T) {
	skipOutgoingRejectStub(t)
	eng, call, _ := testEngineWithIncomingCall()

	rejectFrom(eng, peerDevice(22))

	if call.State() != CallPhaseEnded {
		t.Fatalf("phase = %d, want Ended", call.State())
	}
}
