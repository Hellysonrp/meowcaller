package meowcaller

import (
	"bytes"
	"reflect"
	"testing"
	"time"

	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

func skipMediaHandoffStub(t *testing.T) {
	t.Helper()
	t.Skip("blocked: engine/media-handoff is a stub; enable when implemented")
}

type recordingHandoff struct {
	sessions []MediaSession
	peers    []string
	onStart  func()
}

func (h *recordingHandoff) StartMedia(session MediaSession) {
	h.sessions = append(h.sessions, session)
	if h.onStart != nil {
		h.onStart()
	}
}

func (h *recordingHandoff) PeerChanged(callID, peerLID string) {
	h.peers = append(h.peers, callID+" "+peerLID)
}

func handoffTestRelay() *waBinary.Node {
	return &waBinary.Node{Tag: "relay", Content: []waBinary.Node{
		{Tag: "key", Content: []byte("relay-key")},
		{Tag: "token", Attrs: waBinary.Attrs{"id": "0"}, Content: []byte("token-0")},
		{Tag: "token", Attrs: waBinary.Attrs{"id": "2"}, Content: []byte("token-2")},
		{Tag: "te2", Attrs: waBinary.Attrs{"relay_id": "7", "relay_name": "fna", "token_id": "0", "auth_token_id": "0", "is_fna": "1"},
			Content: []byte{192, 0, 2, 1, 0x0d, 0x96}},
		{Tag: "te2", Attrs: waBinary.Attrs{"relay_id": "8", "relay_name": "own", "token_id": "2", "auth_token_id": "1"},
			Content: []byte{192, 0, 2, 2, 0x0d, 0x98}},
	}}
}

func testEngineWithHandoff(h MediaHandoff, direction CallDirection) (*engine, *Call) {
	c := &Client{mediaHandoff: h}
	c.eng = newEngine(c)
	phase := CallPhaseCalling
	if direction == CallDirectionIncoming {
		phase = CallPhaseRinging
	}
	call := &Call{eng: c.eng, id: "CID", peer: peerJID(), phase: phase}
	c.eng.calls["CID"] = &engineCall{
		call:      call,
		direction: direction,
		from:      peerJID(),
		creator:   creatorJID(),
		callKey:   bytes.Repeat([]byte{0x42}, 32),
		relay:     parseRelayData(handoffTestRelay()),
		selfLID:   creatorJID().String(),
		peerLID:   peerJID().String(),
		codec:     AudioCodecMlow,
	}
	return c.eng, call
}

func TestMediaSessionRelayRoundTrip(t *testing.T) {
	skipMediaHandoffStub(t)
	rd := parseRelayData(handoffTestRelay())

	session := newMediaSessionRelay(rd)
	if len(session.Tokens) != 3 || session.Tokens[1] != nil {
		t.Fatalf("tokens = %q, want three with a nil gap at index 1", session.Tokens)
	}
	if back := session.relayData(); !reflect.DeepEqual(back, rd) {
		t.Fatalf("round trip = %+v, want %+v", back, rd)
	}
	session.Key[0] ^= 0xff
	if rd.relayKeyASCII[0] != 'r' {
		t.Fatal("the session shares its relay key with the engine")
	}
}

func TestMediaHandoffStartsIncomingMediaOnce(t *testing.T) {
	skipMediaHandoffStub(t)
	h := &recordingHandoff{}
	eng, call := testEngineWithHandoff(h, CallDirectionIncoming)

	eng.maybeStartMedia("CID")
	eng.maybeStartMedia("CID")

	if len(h.sessions) != 1 {
		t.Fatalf("StartMedia calls = %d, want 1", len(h.sessions))
	}
	m := eng.calls["CID"]
	want := MediaSession{
		CallID:    "CID",
		CallKey:   m.callKey,
		SelfLID:   m.selfLID,
		PeerLID:   m.peerLID,
		Direction: CallDirectionIncoming,
		Codec:     AudioCodecMlow,
		Relay:     newMediaSessionRelay(m.relay),
	}
	if !reflect.DeepEqual(h.sessions[0], want) {
		t.Fatalf("session = %+v, want %+v", h.sessions[0], want)
	}
	if m.cancel != nil {
		t.Fatal("the handoff started local media")
	}
	if got := call.State(); got != CallPhaseRinging {
		t.Fatalf("phase = %d, want Ringing", got)
	}
}

func TestMediaHandoffStartsOutgoingMediaAsConnecting(t *testing.T) {
	skipMediaHandoffStub(t)
	h := &recordingHandoff{}
	eng, call := testEngineWithHandoff(h, CallDirectionOutgoing)

	eng.maybeStartMedia("CID")

	if len(h.sessions) != 1 || h.sessions[0].Direction != CallDirectionOutgoing {
		t.Fatalf("sessions = %+v, want one outgoing session", h.sessions)
	}
	if got := call.State(); got != CallPhaseConnecting {
		t.Fatalf("phase = %d, want Connecting", got)
	}
}

func TestMediaHandoffSessionOwnsItsBytes(t *testing.T) {
	skipMediaHandoffStub(t)
	h := &recordingHandoff{}
	eng, _ := testEngineWithHandoff(h, CallDirectionIncoming)
	eng.maybeStartMedia("CID")

	session := h.sessions[0]
	session.CallKey[0] ^= 0xff
	session.Relay.Key[0] ^= 0xff
	session.Relay.Tokens[0][0] ^= 0xff

	m := eng.calls["CID"]
	if m.callKey[0] != 0x42 || m.relay.relayKeyASCII[0] != 'r' || m.relay.relayTokens[0][0] != 't' {
		t.Fatal("changing the handed-off session changed the engine's call")
	}
}

func TestMediaHandoffForwardsPeerChange(t *testing.T) {
	skipMediaHandoffStub(t)
	h := &recordingHandoff{}
	eng, call := testEngineWithHandoff(h, CallDirectionOutgoing)
	eng.maybeStartMedia("CID")
	answering := peerJID()
	answering.Device = 7

	eng.onAccept(&events.CallAccept{
		BasicCallMeta: types.BasicCallMeta{CallID: call.ID(), From: answering},
		Data:          &waBinary.Node{Tag: "accept"},
	})

	if want := []string{"CID " + answering.String()}; !reflect.DeepEqual(h.peers, want) {
		t.Fatalf("peer changes = %q, want %q", h.peers, want)
	}
}

func TestMediaHandoffSessionCarriesPeerChangedBeforeStart(t *testing.T) {
	skipMediaHandoffStub(t)
	h := &recordingHandoff{}
	eng, call := testEngineWithHandoff(h, CallDirectionOutgoing)
	m := eng.calls["CID"]
	relay := m.relay
	m.relay = nil
	answering := peerJID()
	answering.Device = 7

	eng.onAccept(&events.CallAccept{
		BasicCallMeta: types.BasicCallMeta{CallID: call.ID(), From: answering},
		Data:          &waBinary.Node{Tag: "accept"},
	})
	eng.mu.Lock()
	m.relay = relay
	eng.mu.Unlock()
	eng.maybeStartMedia("CID")

	if len(h.peers) != 0 {
		t.Fatalf("peer changes before the handoff = %q, want none", h.peers)
	}
	if len(h.sessions) != 1 || h.sessions[0].PeerLID != answering.String() {
		t.Fatalf("sessions = %+v, want one with peer %s", h.sessions, answering)
	}
}

func TestMediaHandoffRunsOutsideEngineLock(t *testing.T) {
	skipMediaHandoffStub(t)
	h := &recordingHandoff{}
	eng, _ := testEngineWithHandoff(h, CallDirectionIncoming)
	h.onStart = func() {
		if eng.lookup("CID") == nil {
			t.Error("the call is missing while StartMedia runs")
		}
	}

	done := make(chan struct{})
	go func() {
		eng.maybeStartMedia("CID")
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("StartMedia ran under the engine lock")
	}
}

func TestMediaHandoffSkipsGroupCalls(t *testing.T) {
	skipMediaHandoffStub(t)
	h := &recordingHandoff{}
	eng, _ := testEngineWithHandoff(h, CallDirectionIncoming)
	m := eng.calls["CID"]
	m.group = true
	m.hasGroupEpoch = true
	m.groupRawEpoch = make([]byte, 32)
	m.groupUpdate = &groupCallUpdate{Relay: &groupCallRelay{}}

	eng.maybeStartMedia("CID")

	if len(h.sessions) != 0 {
		t.Fatalf("StartMedia calls for a group call = %d, want 0", len(h.sessions))
	}
	if !m.started || m.cancel != nil {
		t.Fatal("a group call under a handoff must be marked started without local media")
	}
}
