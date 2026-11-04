package meowcaller

import (
	"context"
	"errors"
	"strings"
	"testing"

	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

const terminateTargetsBlocked = "blocked: engine/terminate-targets hangup fan-out is a stub; enable when implemented"

var errTestSend = errors.New("network unavailable")

// captureSends makes eng record every call node it sends, failing the sends whose
// zero-based index is in fail with errTestSend.
func captureSends(eng *engine, fail ...int) *[]waBinary.Node {
	failing := make(map[int]bool, len(fail))
	for _, index := range fail {
		failing[index] = true
	}
	var sent []waBinary.Node
	eng.sendCallNode = func(_ context.Context, node waBinary.Node) error {
		index := len(sent)
		sent = append(sent, node)
		if failing[index] {
			return errTestSend
		}
		return nil
	}
	return &sent
}

// assertTerminates checks that sent is one <terminate> per JID in want, in order, each
// carrying creator, its own stanza id, no reason and no <destination>.
func assertTerminates(t *testing.T, sent []waBinary.Node, creator types.JID, want ...types.JID) {
	t.Helper()
	if len(sent) != len(want) {
		t.Fatalf("sent %d stanzas, want %d", len(sent), len(want))
	}
	ids := make(map[string]bool, len(sent))
	for i, node := range sent {
		children := node.GetChildren()
		if node.Tag != "call" || len(children) != 1 || children[0].Tag != "terminate" {
			t.Fatalf("stanza %d = %#v, want <call><terminate/></call>", i, node)
		}
		if got := node.AttrGetter().JID("to"); got != want[i] {
			t.Fatalf("terminate %d to = %s, want %s", i, got, want[i])
		}
		id := node.AttrGetter().String("id")
		if id == "" || ids[id] {
			t.Fatalf("terminate %d id = %q, want a non-empty id of its own", i, id)
		}
		ids[id] = true
		terminate := children[0]
		if got := terminate.AttrGetter().JID("call-creator"); got != creator {
			t.Fatalf("terminate %d call-creator = %s, want %s", i, got, creator)
		}
		if _, ok := terminate.Attrs["reason"]; ok {
			t.Fatalf("terminate %d carries a reason: %#v", i, terminate.Attrs)
		}
		if len(terminate.GetChildren()) != 0 {
			t.Fatalf("terminate %d has children %#v, want no <destination>", i, terminate.GetChildren())
		}
	}
}

func acceptFrom(eng *engine, from types.JID) {
	eng.onAccept(&events.CallAccept{
		BasicCallMeta: types.BasicCallMeta{CallID: "CID", From: from},
		Data:          &waBinary.Node{Tag: "accept"},
	})
}

func TestHangupWhileRingingTerminatesEveryRungDevice(t *testing.T) {
	t.Skip(terminateTargetsBlocked)
	eng, call := testEngineWithOutgoingCall()
	eng.calls[call.ID()].ringDevices = []types.JID{peerDevice(0), peerDevice(45)}
	sent := captureSends(eng)

	if err := call.Hangup(); err != nil {
		t.Fatalf("Hangup: %v", err)
	}

	assertTerminates(t, *sent, creatorJID(), peerDevice(0), peerDevice(45))
}

func TestHangupWhileRingingTerminatesALoneDevice(t *testing.T) {
	t.Skip(terminateTargetsBlocked)
	eng, call := testEngineWithOutgoingCall()
	eng.calls[call.ID()].ringDevices = []types.JID{peerDevice(3)}
	sent := captureSends(eng)

	if err := call.Hangup(); err != nil {
		t.Fatalf("Hangup: %v", err)
	}

	assertTerminates(t, *sent, creatorJID(), peerDevice(3))
}

func TestHangupAfterCompanionAcceptTerminatesOnlyThatDevice(t *testing.T) {
	t.Skip(terminateTargetsBlocked)
	eng, call := testEngineWithOutgoingCall()
	eng.calls[call.ID()].ringDevices = []types.JID{peerDevice(0), peerDevice(45)}
	sent := captureSends(eng)

	acceptFrom(eng, peerDevice(45))
	if err := call.Hangup(); err != nil {
		t.Fatalf("Hangup: %v", err)
	}

	assertTerminates(t, *sent, creatorJID(), peerDevice(45))
}

func TestHangupAfterPrimaryAcceptDoesNotFanOut(t *testing.T) {
	t.Skip(terminateTargetsBlocked)
	eng, call := testEngineWithOutgoingCall()
	eng.calls[call.ID()].ringDevices = []types.JID{peerDevice(0), peerDevice(45)}
	sent := captureSends(eng)

	acceptFrom(eng, peerJID())
	if err := call.Hangup(); err != nil {
		t.Fatalf("Hangup: %v", err)
	}

	assertTerminates(t, *sent, creatorJID(), peerJID())
}

func TestHangupOfIncomingCallTerminatesFrom(t *testing.T) {
	eng, call, _ := testEngineWithIncomingCall()
	eng.calls[call.ID()].ringDevices = []types.JID{peerDevice(0), peerDevice(45)}
	sent := captureSends(eng)

	if err := call.Hangup(); err != nil {
		t.Fatalf("Hangup: %v", err)
	}

	assertTerminates(t, *sent, creatorJID(), peerJID())
}

func TestHangupOfGroupCallTerminatesCallScope(t *testing.T) {
	eng, call, creator := testGroupEngine("GROUP")
	m := eng.calls[call.ID()]
	m.direction = CallDirectionOutgoing
	m.ringDevices = []types.JID{peerDevice(0), peerDevice(45)}
	sent := captureSends(eng)

	if err := call.Hangup(); err != nil {
		t.Fatalf("Hangup: %v", err)
	}

	assertTerminates(t, *sent, creator, types.NewJID("GROUP", "call"))
}

func TestHangupWhileRingingKeepsSendingAfterAFailedSend(t *testing.T) {
	t.Skip(terminateTargetsBlocked)
	eng, call := testEngineWithOutgoingCall()
	m := eng.calls[call.ID()]
	var canceled bool
	m.cancel = func() { canceled = true }
	m.ringDevices = []types.JID{peerDevice(0), peerDevice(45)}
	sent := captureSends(eng, 0)
	var reason string
	call.OnEnd(func(got string) { reason = got })

	err := call.Hangup()

	if !errors.Is(err, errTestSend) || !strings.HasPrefix(err.Error(), "send terminate: ") {
		t.Fatalf("Hangup error = %v, want the first send error wrapped as send terminate", err)
	}
	assertTerminates(t, *sent, creatorJID(), peerDevice(0), peerDevice(45))
	if !canceled {
		t.Fatal("Hangup did not cancel local media")
	}
	if call.State() != CallPhaseEnded || reason != "hangup" {
		t.Fatalf("local end state = (%d, %q), want (Ended, hangup)", call.State(), reason)
	}
	if eng.lookup(call.ID()) != nil {
		t.Fatal("Hangup retained ended call in engine registry")
	}
}
