package meowcaller

import (
	"errors"
	"testing"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/store"
	waLog "go.mau.fi/whatsmeow/util/log"
)

func skipImmediateAcceptStub(t *testing.T) {
	t.Helper()
	t.Skip("blocked: engine/immediate-accept is a stub; enable when implemented")
}

func TestImmediateAcceptSendsTheAcceptFromAnswer(t *testing.T) {
	skipImmediateAcceptStub(t)
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
	skipImmediateAcceptStub(t)
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

func TestImmediateAcceptAfterAnEarlyMuteV2SendsOneAccept(t *testing.T) {
	skipImmediateAcceptStub(t)
	eng, call, sent := testEngineSendingAccepts(nil)
	eng.c.immediateAccept = true
	eng.onCallRaw(earlyMuteV2Node())

	if err := eng.answer(call); err != nil {
		t.Fatalf("answer: %v", err)
	}

	if len(*sent) != 1 {
		t.Fatalf("sent %d nodes, want one accept", len(*sent))
	}
}

func TestImmediateAcceptReturnsTheSendError(t *testing.T) {
	skipImmediateAcceptStub(t)
	offline := errors.New("offline")
	eng, call, _ := testEngineSendingAccepts(offline)
	eng.c.immediateAccept = true
	fired := 0
	call.OnAcceptSent(func() { fired++ })

	if err := eng.answer(call); !errors.Is(err, offline) {
		t.Fatalf("answer = %v, want the send error", err)
	}
	if fired != 0 {
		t.Fatalf("OnAcceptSent calls = %d after a failed send, want 0", fired)
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
	skipImmediateAcceptStub(t)
	wa := whatsmeow.NewClient(&store.Device{}, waLog.Noop)

	c := NewClient(wa, WithImmediateAccept())

	if !c.immediateAccept {
		t.Fatal("NewClient did not keep WithImmediateAccept")
	}
}
