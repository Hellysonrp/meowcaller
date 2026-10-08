package meowcaller

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

// fakeRelayChannel is an in-memory relay DataChannel: it keeps what is sent and hands
// Recv what deliver queues.
type fakeRelayChannel struct {
	mu      sync.Mutex
	sent    [][]byte
	sendErr error // every Send fails with it, while Recv keeps working
	in      chan []byte
	closed  chan struct{}
	once    sync.Once
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
	if f.sendErr != nil {
		return 0, f.sendErr
	}
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
	d := newRTPDuplicates()
	if d.has(1, 5) {
		t.Fatal("a first packet was seen")
	}
	d.mark(1, 5)
	if !d.has(1, 5) {
		t.Fatal("a repeated packet was not seen")
	}
	if d.has(2, 5) || d.has(1, 6) {
		t.Fatal("another SSRC's or sequence's packet was seen")
	}
	if d.has(1, 5+1024) {
		t.Fatal("a sequence 1024 later was taken for a repeat")
	}
}

// A copy that never authenticated is not marked, so a later valid copy still goes
// through.
func TestRTPDuplicatesOnlyMarkWhatAuthenticated(t *testing.T) {
	d := newRTPDuplicates()
	if d.has(1, 5) {
		t.Fatal("an unmarked packet was seen")
	}
	if d.has(1, 5) {
		t.Fatal("checking a packet marked it")
	}
}

// The oldest SSRC's window goes once more SSRCs than the bound have been marked.
func TestRTPDuplicatesForgetTheOldestStream(t *testing.T) {
	d := newRTPDuplicates()
	for ssrc := range uint32(maxDuplicateStreams + 1) {
		d.mark(ssrc, 1)
	}
	if d.has(0, 1) {
		t.Fatal("the oldest stream was kept past the bound")
	}
	if !d.has(maxDuplicateStreams, 1) {
		t.Fatal("the newest stream was forgotten")
	}
	if len(d.streams) != maxDuplicateStreams {
		t.Fatalf("kept %d streams, want %d", len(d.streams), maxDuplicateStreams)
	}
}

// An open set with no connection open yet but a dial pending keeps its send loops: the
// dial's connection carries the media once it opens.
func TestRelaySetSendWaitsForAPendingDial(t *testing.T) {
	s := newRelaySet()
	t.Cleanup(func() { s.Close() })
	s.dialing(2)
	a := newFakeRelayChannel()
	s.add(&relayConn{name: "a", ch: a})
	a.Close()
	waitFor(t, "the closed connection was not dropped", func() bool { return s.current() == nil })

	if _, err := s.Send([]byte{0x80, 1}); errors.Is(err, errRelaySetClosed) {
		t.Fatalf("Send = %v while a dial is pending, want a transient error", err)
	}
	b := newFakeRelayChannel()
	s.add(&relayConn{name: "b", ch: b})
	if _, err := s.Send([]byte{0x80, 2}); err != nil || b.sentCount() != 1 {
		t.Fatalf("Send = %v, b sent %d; want the dialed connection to carry the media", err, b.sentCount())
	}
}

// A connection whose writes fail hands the media to another open connection.
func TestRelaySetSendMovesOffAFailingConnection(t *testing.T) {
	s := newRelaySet()
	t.Cleanup(func() { s.Close() })
	a, b := newFakeRelayChannel(), newFakeRelayChannel()
	a.sendErr = io.ErrClosedPipe
	s.add(&relayConn{name: "a", ch: a})
	s.add(&relayConn{name: "b", ch: b})

	if _, err := s.Send([]byte{0x80, 1}); err == nil {
		t.Fatal("a failed write was reported as sent")
	}
	if _, err := s.Send([]byte{0x80, 2}); err != nil || b.sentCount() != 1 {
		t.Fatalf("Send = %v, b sent %d; want the next packet on the open connection", err, b.sentCount())
	}
}

// A group call's connection carries its media and is not moved by the peer's audio.
func TestRelaySetPinnedConnectionIsNotFollowedAway(t *testing.T) {
	s := newRelaySet()
	t.Cleanup(func() { s.Close() })
	first := &relayEndpoint{relayName: "first", addresses: []relayAddress{{ipv4: "10.0.0.1", port: 3478}}}
	second := &relayEndpoint{relayName: "second", addresses: []relayAddress{{ipv4: "10.0.0.2", port: 3478}}}
	a, b := newFakeRelayChannel(), newFakeRelayChannel()
	connA := &relayConn{name: "first", ch: a, endpoint: first}
	connB := &relayConn{name: "second", ch: b, endpoint: second}
	s.add(connA)
	s.add(connB)

	if got := s.byEndpoint(&relayEndpoint{addresses: []relayAddress{{ipv4: "10.0.0.2", port: 3478}}}); got != connB {
		t.Fatalf("byEndpoint = %v, want the connection to that address", got)
	}
	s.pin(connB)
	if s.follow(connA) {
		t.Fatal("follow moved a pinned connection's media")
	}
	if _, err := s.Send([]byte{0x80, 1}); err != nil || b.sentCount() != 1 || a.sentCount() != 0 {
		t.Fatalf("Send = %v, a=%d b=%d; want the pinned connection", err, a.sentCount(), b.sentCount())
	}
}

// Received packets wait for a slow receive loop rather than being dropped: STUN binding
// requests among them must be answered.
func TestRelaySetKeepsEveryPacketForASlowLoop(t *testing.T) {
	s := newRelaySet()
	t.Cleanup(func() { s.Close() })
	a := newFakeRelayChannel()
	s.add(&relayConn{name: "a", ch: a})
	const total = relaySetBacklog + 44
	go func() {
		for i := range total {
			a.deliver([]byte{byte(i), byte(i >> 8)})
		}
	}()
	time.Sleep(200 * time.Millisecond)

	for i := range total {
		data, _, err := recvWithin(t, s)
		if err != nil || len(data) != 2 || int(data[0])|int(data[1])<<8 != i {
			t.Fatalf("packet %d = %v, %v; want every packet in order", i, data, err)
		}
	}
}

// Once the last connection ends, Recv says why.
func TestRelaySetRecvReportsWhyTheLastConnectionEnded(t *testing.T) {
	s := newRelaySet()
	t.Cleanup(func() { s.Close() })
	a := newFakeRelayChannel()
	s.add(&relayConn{name: "a", ch: a})

	a.Close()
	_, _, err := recvWithin(t, s)
	if !errors.Is(err, errRelaySetClosed) || !errors.Is(err, io.EOF) {
		t.Fatalf("Recv = %v, want the set closed because of the connection's EOF", err)
	}
}

// Two entries for one address keep the FNA one, whose token the caller's uplink uses.
func TestRelayTargetsPreferTheFNAEntryForAnAddress(t *testing.T) {
	rd := &relayData{endpoints: []relayEndpoint{
		{relayName: "plain", tokenID: 0, addresses: []relayAddress{{ipv4: "10.0.0.1", port: 3478}}},
		{relayName: "fna", tokenID: 1, isFNA: true, addresses: []relayAddress{{ipv4: "10.0.0.1", port: 3478}}},
		{relayName: "other", tokenID: 2, addresses: []relayAddress{{ipv4: "10.0.0.2", port: 3478}}},
		{relayName: "none"},
	}}

	targets := relayTargets(rd)

	if len(targets) != 2 || targets[0].relayName != "fna" || targets[1].relayName != "other" {
		t.Fatalf("targets = %v, want the FNA entry and the other address", targets)
	}
}

func TestConnectRelaysReportsWhyEveryDialFailed(t *testing.T) {
	shortRelayConnectTimeout(t)
	session := silentRelaySession(t)
	eng, _, _ := testEngineWithIncomingCall()

	_, err := eng.connectRelays(context.Background(), session.Relay.relayData(), [9]uint32{}, [2]uint32{})
	if err == nil || !strings.Contains(err.Error(), "timed out") {
		t.Fatalf("connectRelays = %v, want the dial's own reason", err)
	}
}

func TestOfferedRelayNames(t *testing.T) {
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

// dialDeadline makes a relay that never answers give up quickly.
func dialDeadline(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	t.Cleanup(cancel)
	return ctx
}

func TestDialFirstKeepsTheFirstToOpen(t *testing.T) {
	silent := silentRelaySession(t)
	live, _ := recordingRelaySession(t, 0, nil)
	eng, _, _ := testEngineWithIncomingCall()

	ch, addr, err := eng.dialFirst(dialDeadline(t), []*net.UDPAddr{
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
	silent := silentRelaySession(t)
	eng, _, _ := testEngineWithIncomingCall()

	if _, _, err := eng.dialFirst(dialDeadline(t), []*net.UDPAddr{udpAddr(t, silent.Relay.Endpoints[0])}); err == nil {
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
	eng, _, sent := testEngineSendingAccepts(nil)
	eng.calls["CID"].offerRelayNames = offeredRelayNames(&relayData{endpoints: []relayEndpoint{{relayName: "gru2c01"}}})

	eng.onRelayLatency(relayLatencyEvent("figu2c01", "gru2c01"))

	if len(*sent) != 1 {
		t.Fatalf("answered %d probes, want only the offered relay's", len(*sent))
	}
	te := findChild(&(*sent)[0], "te")
	if te == nil || te.Attrs["relay_name"] != "gru2c01" {
		t.Fatalf("answer = %#v, want gru2c01's", (*sent)[0])
	}
}

// A later stanza's <relay> list replaces m.relay; the answers stay with the offer's.
func TestRelayLatencyFollowsTheOfferNotTheLatestRelayList(t *testing.T) {
	eng, _, sent := testEngineSendingAccepts(nil)
	eng.calls["CID"].offerRelayNames = offeredRelayNames(&relayData{endpoints: []relayEndpoint{{relayName: "gru2c01"}}})
	eng.calls["CID"].relay = &relayData{endpoints: []relayEndpoint{{relayName: "figu2c01"}}}

	eng.onRelayLatency(relayLatencyEvent("figu2c01", "gru2c01"))

	if len(*sent) != 1 || findChild(&(*sent)[0], "te").Attrs["relay_name"] != "gru2c01" {
		t.Fatalf("answers = %#v, want only the offered gru2c01's", *sent)
	}
}

func TestRelayLatencyIsNotAnsweredWithoutAnOfferedRelay(t *testing.T) {
	eng, _, sent := testEngineSendingAccepts(nil)

	eng.onRelayLatency(relayLatencyEvent("figu2c01"))

	if len(*sent) != 0 {
		t.Fatalf("answered %d probes for a call with no offered relay, want none", len(*sent))
	}
}
