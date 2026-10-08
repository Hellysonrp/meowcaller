package meowcaller

import (
	"bytes"
	"context"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

func skipMultiRelayStub(t *testing.T) {
	t.Helper()
	t.Skip("blocked: engine/multi-relay is a stub; enable when implemented")
}

// fakeRelayChannel is an in-memory relay DataChannel: it keeps what is sent and hands
// Recv what deliver queues.
type fakeRelayChannel struct {
	mu     sync.Mutex
	sent   [][]byte
	in     chan []byte
	closed chan struct{}
	once   sync.Once
}

func newFakeRelayChannel() *fakeRelayChannel {
	return &fakeRelayChannel{in: make(chan []byte, 16), closed: make(chan struct{})}
}

func (f *fakeRelayChannel) Send(data []byte) (int, error) {
	select {
	case <-f.closed:
		return 0, io.ErrClosedPipe
	default:
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sent = append(f.sent, bytes.Clone(data))
	return len(data), nil
}

func (f *fakeRelayChannel) Recv(buf []byte) (int, error) {
	select {
	case p := <-f.in:
		return copy(buf, p), nil
	case <-f.closed:
		return 0, io.EOF
	}
}

func (f *fakeRelayChannel) Close() error {
	f.once.Do(func() { close(f.closed) })
	return nil
}

func (f *fakeRelayChannel) deliver(p []byte) { f.in <- p }

func (f *fakeRelayChannel) sentCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.sent)
}

func recvWithin(t *testing.T, s *relaySet) ([]byte, *relayConn, error) {
	t.Helper()
	type result struct {
		data []byte
		conn *relayConn
		err  error
	}
	got := make(chan result, 1)
	go func() {
		buf := make([]byte, 1500)
		n, conn, err := s.Recv(buf)
		got <- result{bytes.Clone(buf[:n]), conn, err}
	}()
	select {
	case r := <-got:
		return r.data, r.conn, r.err
	case <-time.After(2 * time.Second):
		t.Fatal("Recv did not return")
		return nil, nil, nil
	}
}

func TestRelaySetSendsOnTheFirstConnection(t *testing.T) {
	skipMultiRelayStub(t)
	s := newRelaySet()
	t.Cleanup(func() { s.Close() })
	a, b := newFakeRelayChannel(), newFakeRelayChannel()
	s.add(&relayConn{name: "a", ch: a})
	s.add(&relayConn{name: "b", ch: b})

	if _, err := s.Send([]byte{0x80, 1}); err != nil {
		t.Fatalf("Send: %v", err)
	}

	if a.sentCount() != 1 || b.sentCount() != 0 {
		t.Fatalf("sent a=%d b=%d, want the first connection only", a.sentCount(), b.sentCount())
	}
}

func TestRelaySetFollowsThePeer(t *testing.T) {
	skipMultiRelayStub(t)
	s := newRelaySet()
	t.Cleanup(func() { s.Close() })
	a, b := newFakeRelayChannel(), newFakeRelayChannel()
	connB := &relayConn{name: "b", ch: b}
	s.add(&relayConn{name: "a", ch: a})
	s.add(connB)

	s.follow(connB)
	if _, err := s.Send([]byte{0x80, 1}); err != nil {
		t.Fatalf("Send: %v", err)
	}

	if a.sentCount() != 0 || b.sentCount() != 1 {
		t.Fatalf("sent a=%d b=%d, want the followed connection only", a.sentCount(), b.sentCount())
	}
}

func TestRelaySetRecvMergesEveryConnection(t *testing.T) {
	skipMultiRelayStub(t)
	s := newRelaySet()
	t.Cleanup(func() { s.Close() })
	a, b := newFakeRelayChannel(), newFakeRelayChannel()
	connA, connB := &relayConn{name: "a", ch: a}, &relayConn{name: "b", ch: b}
	s.add(connA)
	s.add(connB)

	a.deliver([]byte{1})
	b.deliver([]byte{2})

	from := map[byte]*relayConn{}
	for range 2 {
		data, conn, err := recvWithin(t, s)
		if err != nil || len(data) != 1 {
			t.Fatalf("Recv = %v, %v", data, err)
		}
		from[data[0]] = conn
	}
	if from[1] != connA || from[2] != connB {
		t.Fatalf("packets came from %v, want each from its own connection", from)
	}
}

func TestRelaySetSendingMovesWhenItsConnectionCloses(t *testing.T) {
	skipMultiRelayStub(t)
	s := newRelaySet()
	t.Cleanup(func() { s.Close() })
	a, b := newFakeRelayChannel(), newFakeRelayChannel()
	s.add(&relayConn{name: "a", ch: a})
	s.add(&relayConn{name: "b", ch: b})

	a.Close()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, err := s.Send([]byte{0x80, 1}); err == nil && b.sentCount() == 1 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("sending did not move to the open connection: b sent %d", b.sentCount())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestRelaySetStaysOpenWhileADialIsPending(t *testing.T) {
	skipMultiRelayStub(t)
	s := newRelaySet()
	t.Cleanup(func() { s.Close() })
	s.dialing(2)
	a := newFakeRelayChannel()
	s.add(&relayConn{name: "a", ch: a})

	a.Close()
	time.Sleep(50 * time.Millisecond)
	select {
	case <-s.done:
		t.Fatal("the set closed while a dial was still pending")
	default:
	}

	s.dialFailed()
	if _, _, err := recvWithin(t, s); err == nil {
		t.Fatal("Recv succeeded after every connection closed and every dial failed")
	}
}

func TestRTPDuplicates(t *testing.T) {
	skipMultiRelayStub(t)
	d := newRTPDuplicates()
	if d.seen(1, 5) {
		t.Fatal("a first packet was seen")
	}
	if !d.seen(1, 5) {
		t.Fatal("a repeated packet was not seen")
	}
	if d.seen(2, 5) || d.seen(1, 6) {
		t.Fatal("another SSRC's or sequence's packet was seen")
	}
	if d.seen(1, 5+1024) {
		t.Fatal("a sequence 1024 later was taken for a repeat")
	}
}

func TestOfferedRelayNames(t *testing.T) {
	skipMultiRelayStub(t)
	rd := &relayData{endpoints: []relayEndpoint{{relayName: "gru2c01"}, {relayName: "poa1c01"}, {}}}

	names := offeredRelayNames(rd)

	if len(names) != 2 || !names["gru2c01"] || !names["poa1c01"] {
		t.Fatalf("names = %v, want gru2c01 and poa1c01", names)
	}
	if len(offeredRelayNames(nil)) != 0 {
		t.Fatal("a nil relay offers names")
	}
}

func TestRelayDialAddresses(t *testing.T) {
	skipMultiRelayStub(t)
	offered := relayDialAddresses(&relayEndpoint{addresses: []relayAddress{{ipv4: "57.144.233.57", port: 3478}}})
	if len(offered) != 2 || offered[0].String() != "57.144.233.57:3478" || offered[1].String() != "57.144.233.57:3480" {
		t.Fatalf("addresses = %v, want the offered port then 3480", offered)
	}
	same := relayDialAddresses(&relayEndpoint{addresses: []relayAddress{{ipv4: "170.78.54.98", port: 3480}}})
	if len(same) != 1 || same[0].String() != "170.78.54.98:3480" {
		t.Fatalf("addresses = %v, want 3480 once", same)
	}
	if relayDialAddresses(&relayEndpoint{}) != nil {
		t.Fatal("an endpoint with no address has dial addresses")
	}
}

// shortRelayConnectTimeout makes a relay that never answers give up quickly.
func shortRelayConnectTimeout(t *testing.T) {
	t.Helper()
	previous := relayConnectTimeout
	relayConnectTimeout = 500 * time.Millisecond
	t.Cleanup(func() { relayConnectTimeout = previous })
}

func udpAddr(t *testing.T, ep MediaSessionEndpoint) *net.UDPAddr {
	t.Helper()
	return &net.UDPAddr{IP: net.ParseIP(ep.Addresses[0].IPv4), Port: int(ep.Addresses[0].Port)}
}

func TestDialFirstKeepsTheFirstToOpen(t *testing.T) {
	skipMultiRelayStub(t)
	shortRelayConnectTimeout(t)
	silent := silentRelaySession(t)
	live, _ := recordingRelaySession(t, 0, nil)
	eng, _, _ := testEngineWithIncomingCall()

	ch, addr, err := eng.dialFirst(context.Background(), []*net.UDPAddr{
		udpAddr(t, silent.Relay.Endpoints[0]), udpAddr(t, live.Relay.Endpoints[0]),
	})
	if err != nil {
		t.Fatalf("dialFirst: %v", err)
	}
	t.Cleanup(func() { ch.Close() })
	if addr.String() != udpAddr(t, live.Relay.Endpoints[0]).String() {
		t.Fatalf("kept %v, want the relay that answers", addr)
	}
}

func TestDialFirstFailsWhenNothingOpens(t *testing.T) {
	skipMultiRelayStub(t)
	shortRelayConnectTimeout(t)
	silent := silentRelaySession(t)
	eng, _, _ := testEngineWithIncomingCall()

	if _, _, err := eng.dialFirst(context.Background(), []*net.UDPAddr{udpAddr(t, silent.Relay.Endpoints[0])}); err == nil {
		t.Fatal("dialFirst succeeded with no relay answering")
	}
}

// relayPackets counts what a loopback relay reads: media (RTP and SRTCP, version 2)
// and everything else (the allocate and the pings).
type relayPackets struct {
	media, other atomic.Int64
}

func (r *relayPackets) record(packet []byte) {
	if len(packet) > 0 && packet[0]&0xC0 == 0x80 {
		r.media.Add(1)
		return
	}
	r.other.Add(1)
}

// twoRelaySession is a session offering two loopback relays, each recording what it reads.
func twoRelaySession(t *testing.T) (MediaSession, *relayPackets, *relayPackets) {
	t.Helper()
	first, second := &relayPackets{}, &relayPackets{}
	session, _ := recordingRelaySession(t, 0, first.record)
	other, _ := recordingRelaySession(t, 0, second.record)
	session.Relay.Endpoints[0].RelayName = "first"
	endpoint := other.Relay.Endpoints[0]
	endpoint.RelayID, endpoint.RelayName, endpoint.IsFNA = 8, "second", false
	session.Relay.Endpoints = append(session.Relay.Endpoints, endpoint)
	return session, first, second
}

func waitFor(t *testing.T, what string, done func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !done() {
		if time.Now().After(deadline) {
			t.Fatal(what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestRunMediaConnectsEveryOfferedRelay(t *testing.T) {
	skipMultiRelayStub(t)
	session, first, second := twoRelaySession(t)

	mc, err := RunMedia(context.Background(), session)
	if err != nil {
		t.Fatalf("RunMedia: %v", err)
	}
	t.Cleanup(func() { mc.Stop(); waitMediaDone(t, mc) })

	waitFor(t, "a relay was never allocated", func() bool { return first.other.Load() > 0 && second.other.Load() > 0 })
	waitFor(t, "no media reached either relay", func() bool { return first.media.Load()+second.media.Load() > 0 })
	time.Sleep(300 * time.Millisecond)
	if first.media.Load() > 0 && second.media.Load() > 0 {
		t.Fatalf("media reached both relays (first %d, second %d), want one", first.media.Load(), second.media.Load())
	}
}

func TestConnectRelaysStartsOnTheFirstToOpen(t *testing.T) {
	skipMultiRelayStub(t)
	shortRelayConnectTimeout(t)
	live := &relayPackets{}
	session, _ := recordingRelaySession(t, 0, live.record)
	silent := silentRelaySession(t)
	endpoint := silent.Relay.Endpoints[0]
	endpoint.RelayID, endpoint.RelayName = 9, "silent"
	session.Relay.Endpoints = append([]MediaSessionEndpoint{endpoint}, session.Relay.Endpoints...)
	eng, _, _ := testEngineWithIncomingCall()

	started := time.Now()
	set, err := eng.connectRelays(context.Background(), session.Relay.relayData(), [9]uint32{}, [2]uint32{})
	if err != nil {
		t.Fatalf("connectRelays: %v", err)
	}
	t.Cleanup(func() { set.Close() })
	if elapsed := time.Since(started); elapsed >= relayConnectTimeout {
		t.Fatalf("connectRelays took %v, want it to return on the first relay to open", elapsed)
	}
	waitFor(t, "the open relay never got its allocate", func() bool { return live.other.Load() >= 2 })
}

func relayLatencyEvent(names ...string) *events.CallRelayLatency {
	var tes []waBinary.Node
	for _, name := range names {
		tes = append(tes, waBinary.Node{
			Tag:     "te",
			Attrs:   waBinary.Attrs{"latency": "33554460", "relay_name": name},
			Content: []byte{1, 2, 3, 4, 0x0d, 0x96},
		})
	}
	return &events.CallRelayLatency{
		BasicCallMeta: types.BasicCallMeta{From: peerJID(), CallCreator: creatorJID(), CallID: "CID"},
		Data: &waBinary.Node{Tag: "relaylatency", Attrs: waBinary.Attrs{
			"call-id": "CID", "call-creator": creatorJID(),
		}, Content: tes},
	}
}

func TestRelayLatencyIsAnsweredOnlyForOfferedRelays(t *testing.T) {
	skipMultiRelayStub(t)
	eng, _, sent := testEngineSendingAccepts(nil)
	eng.calls["CID"].relay = &relayData{endpoints: []relayEndpoint{{relayName: "gru2c01"}}}

	eng.onRelayLatency(relayLatencyEvent("figu2c01", "gru2c01"))

	if len(*sent) != 1 {
		t.Fatalf("answered %d probes, want only the offered relay's", len(*sent))
	}
	te := findChild(&(*sent)[0], "te")
	if te == nil || te.Attrs["relay_name"] != "gru2c01" {
		t.Fatalf("answer = %#v, want gru2c01's", (*sent)[0])
	}
}

func TestRelayLatencyIsNotAnsweredWithoutAnOfferedRelay(t *testing.T) {
	skipMultiRelayStub(t)
	eng, _, sent := testEngineSendingAccepts(nil)

	eng.onRelayLatency(relayLatencyEvent("figu2c01"))

	if len(*sent) != 0 {
		t.Fatalf("answered %d probes for a call with no offered relay, want none", len(*sent))
	}
}
