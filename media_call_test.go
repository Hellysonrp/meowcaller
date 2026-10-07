package meowcaller

import (
	"bytes"
	"context"
	"errors"
	"net"
	"reflect"
	"testing"
	"time"
)

// silentRelaySession is a session whose only relay endpoint is a local UDP socket that
// never answers, so the media loop stays in its relay connect until stopped.
func silentRelaySession(t *testing.T) MediaSession {
	t.Helper()
	conn, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	port := conn.LocalAddr().(*net.UDPAddr).Port
	return MediaSession{
		CallID:    "CID",
		CallKey:   bytes.Repeat([]byte{0x42}, 32),
		SelfLID:   creatorJID().String(),
		PeerLID:   peerJID().String(),
		Direction: CallDirectionIncoming,
		Codec:     AudioCodecMlow,
		Relay: MediaSessionRelay{
			Key:    []byte("relay-key"),
			Tokens: [][]byte{[]byte("token-0")},
			Endpoints: []MediaSessionEndpoint{{
				RelayID: 7, RelayName: "silent", TokenID: 0, IsFNA: true,
				Addresses: []MediaSessionAddress{{IPv4: "127.0.0.1", Port: uint16(port)}},
			}},
		},
	}
}

func waitMediaDone(t *testing.T, mc *MediaCall) {
	t.Helper()
	select {
	case <-mc.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("the media did not end")
	}
}

func TestRunMediaRejectsIncompleteSessions(t *testing.T) {
	cases := map[string]func(*MediaSession){
		"no call ID":  func(s *MediaSession) { s.CallID = "" },
		"no call key": func(s *MediaSession) { s.CallKey = nil },
		"no peer LID": func(s *MediaSession) { s.PeerLID = "" },
		"no endpoint": func(s *MediaSession) { s.Relay.Endpoints = nil },
	}
	for name, mutate := range cases {
		session := silentRelaySession(t)
		mutate(&session)
		if mc, err := RunMedia(context.Background(), session); err == nil || mc != nil {
			t.Fatalf("%s: RunMedia = (%v, %v), want an error", name, mc, err)
		}
	}
}

func TestRunMediaBuildsCallFromSessionAndStops(t *testing.T) {
	session := silentRelaySession(t)
	wantRelay := session.Relay.relayData()

	mc, err := RunMedia(context.Background(), session)
	if err != nil {
		t.Fatalf("RunMedia: %v", err)
	}
	session.CallKey[0] ^= 0xff
	session.Relay.Key[0] ^= 0xff

	mc.eng.mu.Lock()
	m := mc.eng.calls["CID"]
	if m == nil {
		mc.eng.mu.Unlock()
		t.Fatal("RunMedia registered no call")
	}
	ok := bytes.Equal(m.callKey, bytes.Repeat([]byte{0x42}, 32)) &&
		reflect.DeepEqual(m.relay, wantRelay) &&
		m.selfLID == creatorJID().String() && m.peerLID == peerJID().String() &&
		m.direction == CallDirectionIncoming && m.codec == AudioCodecMlow &&
		m.answered && m.started && m.cancel != nil
	mc.eng.mu.Unlock()
	if !ok {
		t.Fatal("the registered call does not match the session")
	}
	if got := mc.call.State(); got != CallPhaseConnecting {
		t.Fatalf("phase = %d, want Connecting", got)
	}
	if err := mc.Err(); err != nil {
		t.Fatalf("Err before Done = %v, want nil", err)
	}

	mc.Stop()
	mc.Stop()
	waitMediaDone(t, mc)

	if err := mc.Err(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Err = %v, want context.Canceled", err)
	}
	if got := mc.call.State(); got != CallPhaseEnded {
		t.Fatalf("phase after Stop = %d, want Ended", got)
	}
	if mc.eng.lookup("CID") != nil {
		t.Fatal("the ended call is still registered")
	}
}

func TestRunMediaEndsOnCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	mc, err := RunMedia(ctx, silentRelaySession(t))
	if err != nil {
		t.Fatalf("RunMedia: %v", err)
	}
	waitMediaDone(t, mc)

	if err := mc.Err(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Err = %v, want context.Canceled", err)
	}
}

func TestMediaCallAttachesAudio(t *testing.T) {
	mc, err := RunMedia(context.Background(), silentRelaySession(t))
	if err != nil {
		t.Fatalf("RunMedia: %v", err)
	}
	t.Cleanup(func() { mc.Stop(); waitMediaDone(t, mc) })
	sink := &lifecycleAudioSink{}

	player := mc.Play(&lifecycleAudioSource{})
	mc.Receive(sink)

	gotPlayer, gotSink := mc.call.playerAndSink()
	if gotPlayer != player || gotSink != sink {
		t.Fatal("Play and Receive did not attach to the call")
	}
}

func TestMediaCallRekeyBeforeLoopUpdatesPeer(t *testing.T) {
	mc, err := RunMedia(context.Background(), silentRelaySession(t))
	if err != nil {
		t.Fatalf("RunMedia: %v", err)
	}
	t.Cleanup(func() { mc.Stop(); waitMediaDone(t, mc) })
	device := peerJID()
	device.Device = 7

	if err := mc.Rekey(device.String()); err != nil {
		t.Fatalf("Rekey: %v", err)
	}
	if err := mc.Rekey(""); err != nil {
		t.Fatalf("Rekey with an empty peer: %v", err)
	}
	if err := mc.Rekey(device.String()); err != nil {
		t.Fatalf("Rekey to the current peer: %v", err)
	}

	mc.eng.mu.Lock()
	got := mc.eng.calls["CID"].peerLID
	mc.eng.mu.Unlock()
	if got != device.String() {
		t.Fatalf("peer = %q, want %q", got, device.String())
	}
}

func TestMediaCallRekeyAfterEndFails(t *testing.T) {
	mc, err := RunMedia(context.Background(), silentRelaySession(t))
	if err != nil {
		t.Fatalf("RunMedia: %v", err)
	}
	mc.Stop()
	waitMediaDone(t, mc)
	device := peerJID()
	device.Device = 9

	if err := mc.Rekey(device.String()); err == nil {
		t.Fatal("Rekey after the media ended succeeded")
	}
}
