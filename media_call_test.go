package meowcaller

import (
	"bytes"
	"context"
	"errors"
	"net"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pion/datachannel"
	"github.com/pion/dtls/v3"
	"github.com/pion/dtls/v3/pkg/crypto/selfsign"
	"github.com/pion/logging"
	"github.com/pion/sctp"
	"github.com/rs/zerolog"

	"github.com/purpshell/meowcaller/relay"
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

// quietRelaySession is a session whose relay endpoint completes the DTLS, SCTP and
// DataChannel handshake, reads everything it is sent, and never sends anything back.
// The returned channel closes once the relay has read enough packets that the media
// loop is waiting on its receive.
func quietRelaySession(t *testing.T) (MediaSession, <-chan struct{}) {
	t.Helper()
	return loopbackRelaySession(t, 0)
}

// loopbackRelaySession is quietRelaySession whose relay hangs up after reading
// closeAfter packets; zero keeps it open.
func loopbackRelaySession(t *testing.T, closeAfter int) (MediaSession, <-chan struct{}) {
	t.Helper()
	cert, err := selfsign.GenerateSelfSigned()
	if err != nil {
		t.Fatalf("relay cert: %v", err)
	}
	ln, err := dtls.ListenWithOptions("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)}, dtls.WithCertificates(cert))
	if err != nil {
		t.Fatalf("relay listen: %v", err)
	}
	accepted := make(chan net.Conn, 1)
	t.Cleanup(func() {
		ln.Close()
		select {
		case conn := <-accepted:
			conn.Close()
		default:
		}
	})
	flowing := make(chan struct{})
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		accepted <- conn
		assoc, err := sctp.ServerWithOptions(sctp.WithNetConn(conn), sctp.WithName("quiet-relay"))
		if err != nil {
			return
		}
		defer assoc.Close()
		dc, err := datachannel.Dial(assoc, 0, &datachannel.Config{
			Negotiated:    true,
			Label:         relay.DataChannelLabel,
			LoggerFactory: logging.NewDefaultLoggerFactory(),
		})
		if err != nil {
			return
		}
		buf := make([]byte, 1500)
		for read := 0; ; read++ {
			if _, err := dc.Read(buf); err != nil {
				return
			}
			if read == 20 {
				close(flowing)
			}
			if closeAfter > 0 && read+1 == closeAfter {
				return
			}
		}
	}()
	session := silentRelaySession(t)
	session.Relay.Endpoints[0].Addresses[0].Port = uint16(ln.Addr().(*net.UDPAddr).Port)
	return session, flowing
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

func TestRunMediaStopEndsConnectedMedia(t *testing.T) {
	session, flowing := quietRelaySession(t)
	mc, err := RunMedia(context.Background(), session)
	if err != nil {
		t.Fatalf("RunMedia: %v", err)
	}
	select {
	case <-flowing:
	case <-mc.Done():
		t.Fatalf("the media ended before it connected: %v", mc.Err())
	case <-time.After(10 * time.Second):
		mc.Stop()
		waitMediaDone(t, mc)
		t.Fatal("the media never reached the relay")
	}

	mc.Stop()
	waitMediaDone(t, mc)

	if err := mc.Err(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Err = %v, want context.Canceled", err)
	}
}

// syncBuffer is a log sink safe for the media goroutines to write while a test reads it.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestLocalMediaHangupIsNotAWarning(t *testing.T) {
	session, flowing := quietRelaySession(t)
	eng, _, _ := testEngineWithIncomingCall()
	logs := &syncBuffer{}
	eng.c.log = zerolog.New(logs).Level(zerolog.DebugLevel)
	m := eng.calls["CID"]
	m.callKey = session.CallKey
	m.relay = session.Relay.relayData()
	m.selfLID = session.SelfLID
	m.peerLID = session.PeerLID

	eng.maybeStartMedia("CID")
	select {
	case <-flowing:
	case <-time.After(10 * time.Second):
		eng.finishCall("CID", "test timeout")
		t.Fatal("the media never reached the relay")
	}
	eng.finishCall("CID", "hangup")

	deadline := time.Now().Add(5 * time.Second)
	for !strings.Contains(logs.String(), `"message":"media stopped"`) {
		if time.Now().After(deadline) {
			t.Fatalf("no media-stopped line after hangup; logs:\n%s", logs.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
	if strings.Contains(logs.String(), `"message":"media ended"`) {
		t.Fatalf("a normal hangup logged media ended; logs:\n%s", logs.String())
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
