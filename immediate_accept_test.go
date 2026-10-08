package meowcaller

import (
	"context"
	"errors"
	"testing"

	"go.mau.fi/whatsmeow"
	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
	waLog "go.mau.fi/whatsmeow/util/log"
)

func TestImmediateAcceptSendsTheAcceptFromAnswer(t *testing.T) {
	eng, call, sent := testEngineSendingAccepts(nil)
	eng.c.immediateAccept = true
	fired := 0
	call.OnAcceptSent(func() { fired++ })

	if err := eng.answer(call); err != nil {
		t.Fatalf("answer: %v", err)
	}

	if len(*sent) != 1 || (*sent)[0].GetChildren()[0].Tag != "accept" {
		t.Fatalf("sent = %#v, want one accept from Answer", *sent)
	}
	if to := (*sent)[0].Attrs["to"]; to != peerJID() {
		t.Fatalf("accept to = %v, want the offer's sender %v", to, peerJID())
	}
	if fired != 1 {
		t.Fatalf("OnAcceptSent calls = %d, want 1", fired)
	}
}

func TestImmediateAcceptIgnoresALaterMuteV2(t *testing.T) {
	eng, call, sent := testEngineSendingAccepts(nil)
	eng.c.immediateAccept = true
	if err := eng.answer(call); err != nil {
		t.Fatalf("answer: %v", err)
	}

	eng.onCallRaw(earlyMuteV2Node())

	if len(*sent) != 1 {
		t.Fatalf("sent %d nodes, want only the accept from Answer", len(*sent))
	}
}

// An early mute_v2 from another of the caller's devices addresses the accept to that
// device, as the deferred accept would be.
func TestImmediateAcceptAfterAnEarlyMuteV2GoesToItsSender(t *testing.T) {
	eng, call, sent := testEngineSendingAccepts(nil)
	eng.c.immediateAccept = true
	mutingDevice := types.JID{User: "222222222222222", Server: types.HiddenUserServer, Device: 7}
	mute := earlyMuteV2Node()
	mute.Attrs["from"] = mutingDevice
	eng.onCallRaw(mute)

	if err := eng.answer(call); err != nil {
		t.Fatalf("answer: %v", err)
	}

	if len(*sent) != 1 {
		t.Fatalf("sent %d nodes, want one accept", len(*sent))
	}
	if to := (*sent)[0].Attrs["to"]; to != mutingDevice {
		t.Fatalf("accept to = %v, want the early mute_v2's sender %v", to, mutingDevice)
	}
	if got := (*sent)[0].GetChildren()[0].Attrs["call-creator"]; got != creatorJID() {
		t.Fatalf("accept call-creator = %v, want %v", got, creatorJID())
	}
}

// A failed immediate accept fails Answer and leaves the call ringing, so another Answer
// sends it again.
func TestImmediateAcceptReturnsTheSendErrorAndKeepsTheCallRinging(t *testing.T) {
	offline := errors.New("offline")
	sendErr := offline
	eng, call, _ := testEngineSendingAccepts(nil)
	var sent int
	eng.sendCallNode = func(context.Context, waBinary.Node) error {
		sent++
		return sendErr
	}
	eng.c.immediateAccept = true
	fired := 0
	call.OnAcceptSent(func() { fired++ })

	if err := eng.answer(call); !errors.Is(err, offline) {
		t.Fatalf("answer = %v, want the send error", err)
	}
	if fired != 0 || sent != 1 || call.State() != CallPhaseRinging {
		t.Fatalf("after a failed send: fired %d, sent %d, phase %v; want 0, 1, ringing", fired, sent, call.State())
	}
	eng.onFirstInboundRTP("CID", call)
	if call.State() != CallPhaseRinging {
		t.Fatalf("phase after inbound RTP = %v, want ringing while unanswered", call.State())
	}

	sendErr = nil
	if err := eng.answer(call); err != nil {
		t.Fatalf("second answer: %v", err)
	}
	if fired != 1 || sent != 2 || call.State() != CallPhaseActive {
		t.Fatalf("after the retry: fired %d, sent %d, phase %v; want 1, 2, active", fired, sent, call.State())
	}
}

func TestImmediateAcceptRepeatedAnswerSendsNothingMore(t *testing.T) {
	eng, call, sent := testEngineSendingAccepts(nil)
	eng.c.immediateAccept = true

	if err := eng.answer(call); err != nil {
		t.Fatalf("answer: %v", err)
	}
	if err := eng.answer(call); err != nil {
		t.Fatalf("repeated answer: %v", err)
	}

	if len(*sent) != 1 {
		t.Fatalf("sent %d nodes, want one accept", len(*sent))
	}
}

func TestImmediateAcceptRefusesAnOutgoingCall(t *testing.T) {
	eng, call, sent := testEngineSendingAccepts(nil)
	eng.c.immediateAccept = true
	eng.calls["CID"].direction = CallDirectionOutgoing

	if err := eng.answer(call); err == nil {
		t.Fatal("answer on an outgoing call succeeded")
	}
	if len(*sent) != 0 {
		t.Fatalf("sent %d nodes for an outgoing call, want none", len(*sent))
	}
}

func TestWithoutImmediateAcceptAnswerStillWaitsForMuteV2(t *testing.T) {
	eng, call, sent := testEngineSendingAccepts(nil)

	if err := eng.answer(call); err != nil {
		t.Fatalf("answer: %v", err)
	}

	if len(*sent) != 0 {
		t.Fatalf("sent %d nodes from Answer, want the accept deferred to mute_v2", len(*sent))
	}
}

func TestWithImmediateAcceptReachesNewClient(t *testing.T) {
	wa := whatsmeow.NewClient(&store.Device{}, waLog.Noop)

	c := NewClient(wa, WithImmediateAccept())

	if !c.immediateAccept {
		t.Fatal("NewClient did not keep WithImmediateAccept")
	}
}
