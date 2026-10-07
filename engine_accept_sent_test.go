package meowcaller

import (
	"context"
	"errors"
	"testing"

	waBinary "go.mau.fi/whatsmeow/binary"
)

func earlyMuteV2Node() *waBinary.Node {
	return &waBinary.Node{
		Tag:   "call",
		Attrs: waBinary.Attrs{"from": peerJID(), "id": "N1"},
		Content: []waBinary.Node{{Tag: "mute_v2", Attrs: waBinary.Attrs{
			"call-id": "CID", "call-creator": creatorJID(), "mute-state": "0",
		}}},
	}
}

func testEngineSendingAccepts(sendErr error) (*engine, *Call, *[]waBinary.Node) {
	eng, call, _ := testEngineWithIncomingCall()
	var sent []waBinary.Node
	eng.sendCallNode = func(_ context.Context, node waBinary.Node) error {
		sent = append(sent, node)
		return sendErr
	}
	eng.newRequestID = func() string { return "REQ" }
	return eng, call, &sent
}

func TestAcceptSentFiresWhenAnswerSendsEarlyMuteAccept(t *testing.T) {
	eng, call, sent := testEngineSendingAccepts(nil)
	fired := 0
	call.OnAcceptSent(func() { fired++ })

	eng.onCallRaw(earlyMuteV2Node())
	if err := eng.answer(call); err != nil {
		t.Fatalf("answer: %v", err)
	}

	if len(*sent) != 1 || (*sent)[0].Attrs["id"] != "REQ" || (*sent)[0].GetChildren()[0].Tag != "accept" {
		t.Fatalf("sent = %#v, want one accept with id REQ", *sent)
	}
	if fired != 1 {
		t.Fatalf("OnAcceptSent calls = %d, want 1", fired)
	}
}

func TestAcceptSentWaitsForMuteAfterAnswer(t *testing.T) {
	eng, call, sent := testEngineSendingAccepts(nil)
	fired := 0
	call.OnAcceptSent(func() { fired++ })

	if err := eng.answer(call); err != nil {
		t.Fatalf("answer: %v", err)
	}
	if len(*sent) != 0 || fired != 0 {
		t.Fatalf("after answer: sent %d, fired %d; want nothing before mute_v2", len(*sent), fired)
	}
	eng.onCallRaw(earlyMuteV2Node())

	if len(*sent) != 1 || fired != 1 {
		t.Fatalf("after mute_v2: sent %d, fired %d; want one accept reported once", len(*sent), fired)
	}
}

func TestAcceptSentSilentOnSendError(t *testing.T) {
	eng, call, sent := testEngineSendingAccepts(errors.New("offline"))
	fired := 0
	call.OnAcceptSent(func() { fired++ })

	if err := eng.answer(call); err != nil {
		t.Fatalf("answer: %v", err)
	}
	eng.onCallRaw(earlyMuteV2Node())

	if len(*sent) != 1 || fired != 0 {
		t.Fatalf("sent %d, fired %d; want one failed send and no report", len(*sent), fired)
	}
}

func TestAcceptSentLateListenerRunsOnceAtRegistration(t *testing.T) {
	eng, call, _ := testEngineSendingAccepts(nil)
	if err := eng.answer(call); err != nil {
		t.Fatalf("answer: %v", err)
	}
	eng.onCallRaw(earlyMuteV2Node())
	fired := 0

	call.OnAcceptSent(func() { fired++ })
	if fired != 1 {
		t.Fatalf("late registration calls = %d, want 1", fired)
	}
	eng.onCallRaw(earlyMuteV2Node())
	call.markAcceptSent()

	if fired != 1 {
		t.Fatalf("OnAcceptSent calls = %d, want it never repeated", fired)
	}
}

func TestAcceptWithoutRequestIDSourceStillCarriesAnID(t *testing.T) {
	eng, call, sent := testEngineSendingAccepts(nil)
	eng.newRequestID = nil

	if err := eng.answer(call); err != nil {
		t.Fatalf("answer: %v", err)
	}
	eng.onCallRaw(earlyMuteV2Node())

	if len(*sent) != 1 || (*sent)[0].Attrs["id"] == "" {
		t.Fatalf("sent = %#v, want one accept with a non-empty id", *sent)
	}
}

func TestAcceptSentFiresForGroupAnswerBeforeConnecting(t *testing.T) {
	eng, call, _ := testGroupEngine("GROUP")
	eng.sendCallNode = func(context.Context, waBinary.Node) error { return nil }
	fired := 0
	var phaseAtFire CallPhase
	call.OnAcceptSent(func() {
		fired++
		phaseAtFire = call.State()
	})

	if err := call.Answer(); err != nil {
		t.Fatalf("Answer: %v", err)
	}

	if fired != 1 {
		t.Fatalf("OnAcceptSent calls = %d, want 1", fired)
	}
	if phaseAtFire != CallPhaseRinging {
		t.Fatalf("phase when reported = %d, want Ringing (before Connecting)", phaseAtFire)
	}
}
