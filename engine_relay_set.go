package meowcaller

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/purpshell/meowcaller/relay"
	"github.com/purpshell/meowcaller/stun"
)

// webClientRelayPort is the port WhatsApp Web dials on every relay.
const webClientRelayPort = 3480

// relayConnectTimeout bounds one relay connection's dial and handshake.
var relayConnectTimeout = 12 * time.Second

var (
	errRelaySetClosed = errors.New("meowcaller: no relay connection is open")
	// errNoRelaySending is a send while no connection is open yet but a dial is pending:
	// that dial's connection carries the media once it opens.
	errNoRelaySending = errors.New("meowcaller: no relay connection is open yet")
)

// relaySender is where a call's media packets go: its relay set.
type relaySender interface {
	Send(data []byte) (int, error)
}

// relayChannel is the part of a relay DataChannel a call's media uses.
type relayChannel interface {
	Send(data []byte) (int, error)
	Recv(buf []byte) (int, error)
	Close() error
}

// relayConn is one relay connection of a call: its DataChannel and its own allocate.
type relayConn struct {
	name     string
	endpoint *relayEndpoint
	ch       relayChannel
	allocate *groupRelayAllocateState
}

// send writes one packet on this connection.
func (c *relayConn) send(packet []byte) error {
	// Source of truth: https://github.com/Hellysonrp/meowcaller/blob/3c3a9e04ec5b78165c1cf5ceedbe7335ca304fab/datasheets/multi-relay.md#L31
	_, err := c.ch.Send(packet)
	return err
}

// relayPacket is one packet a connection received.
type relayPacket struct {
	conn *relayConn
	data []byte
}

// relaySet is a call's relay connections: packets arrive from all of them, and the
// call's media is sent on one.
type relaySet struct {
	mu      sync.Mutex
	conns   []*relayConn
	sending *relayConn
	pinned  bool // a group call's connection carries the media whatever arrives elsewhere
	pending int
	closed  bool
	lastErr error // why the last connection to end ended

	packets   chan relayPacket
	done      chan struct{}
	closeOnce sync.Once
}

// relaySetBacklog bounds the received packets waiting for the receive loop. A full
// backlog drops a packet rather than stall a connection's reader: live media recovers on
// the next frame.
const relaySetBacklog = 256

// relayRecvBufferBytes is a received packet's largest size, as the receive loop reads it.
const relayRecvBufferBytes = 1500

func newRelaySet() *relaySet {
	// Source of truth: https://github.com/Hellysonrp/meowcaller/blob/3c3a9e04ec5b78165c1cf5ceedbe7335ca304fab/datasheets/multi-relay.md#L32
	return &relaySet{packets: make(chan relayPacket, relaySetBacklog), done: make(chan struct{})}
}

// add joins a connection that opened: it reads from then on, and carries the media when
// it is the first. A pending dial that opened counts as done; a set already closed closes
// the connection.
func (s *relaySet) add(c *relayConn) {
	// Source of truth: https://github.com/Hellysonrp/meowcaller/blob/3c3a9e04ec5b78165c1cf5ceedbe7335ca304fab/datasheets/multi-relay.md#L30
	s.mu.Lock()
	if s.pending > 0 {
		s.pending--
	}
	if s.closed {
		s.mu.Unlock()
		_ = c.ch.Close()
		return
	}
	s.conns = append(s.conns, c)
	if s.sending == nil {
		s.sending = c
	}
	s.mu.Unlock()
	go s.read(c)
}

// read hands every packet c receives to the receive loop, until c fails. A slow loop holds
// the reader back, as the single DataChannel's own buffering did, rather than lose a
// packet: STUN binding requests among them must be answered.
func (s *relaySet) read(c *relayConn) {
	// Source of truth: https://github.com/Hellysonrp/meowcaller/blob/3c3a9e04ec5b78165c1cf5ceedbe7335ca304fab/datasheets/multi-relay.md#L32
	buf := make([]byte, relayRecvBufferBytes)
	for {
		n, err := c.ch.Recv(buf)
		if err != nil {
			s.drop(c, err)
			return
		}
		select {
		case s.packets <- relayPacket{conn: c, data: bytes.Clone(buf[:n])}:
		case <-s.done:
			return
		}
	}
}

// drop takes a connection whose reader stopped with err out of the set, moving the media
// to another open connection when it was sending; the set closes when no connection is
// open and no dial is pending.
func (s *relaySet) drop(c *relayConn, err error) {
	// Source of truth: https://github.com/Hellysonrp/meowcaller/blob/3c3a9e04ec5b78165c1cf5ceedbe7335ca304fab/datasheets/multi-relay.md#L33-L34
	s.mu.Lock()
	s.conns = slices.DeleteFunc(s.conns, func(open *relayConn) bool { return open == c })
	if s.sending == c {
		s.sending = nil
		s.pinned = false
		if len(s.conns) > 0 {
			s.sending = s.conns[0]
		}
	}
	s.lastErr = err
	last := len(s.conns) == 0 && s.pending == 0
	s.mu.Unlock()
	_ = c.ch.Close()
	if last {
		_ = s.Close()
	}
}

// dialing records n dials still pending: the set stays open for them.
func (s *relaySet) dialing(n int) {
	// Source of truth: https://github.com/Hellysonrp/meowcaller/blob/3c3a9e04ec5b78165c1cf5ceedbe7335ca304fab/datasheets/multi-relay.md#L34
	s.mu.Lock()
	s.pending += n
	s.mu.Unlock()
}

// dialFailed records a pending dial that gave up.
func (s *relaySet) dialFailed() {
	// Source of truth: https://github.com/Hellysonrp/meowcaller/blob/3c3a9e04ec5b78165c1cf5ceedbe7335ca304fab/datasheets/multi-relay.md#L30
	s.mu.Lock()
	if s.pending > 0 {
		s.pending--
	}
	last := len(s.conns) == 0 && s.pending == 0
	s.mu.Unlock()
	if last {
		_ = s.Close()
	}
}

// Send writes one media packet on the sending connection. errRelaySetClosed means no
// connection is left; errNoRelaySending, that none is open yet while a dial is pending.
// A write that fails costs that packet and hands the media to another open connection.
func (s *relaySet) Send(packet []byte) (int, error) {
	// Source of truth: https://github.com/Hellysonrp/meowcaller/blob/3c3a9e04ec5b78165c1cf5ceedbe7335ca304fab/datasheets/multi-relay.md#L33
	s.mu.Lock()
	closed, c := s.closed, s.sending
	s.mu.Unlock()
	switch {
	case closed:
		return 0, errRelaySetClosed
	case c == nil:
		return 0, errNoRelaySending
	}
	n, err := c.ch.Send(packet)
	if err != nil {
		s.mu.Lock()
		if s.sending == c {
			if i := slices.IndexFunc(s.conns, func(open *relayConn) bool { return open != c }); i >= 0 {
				s.sending, s.pinned = s.conns[i], false
			}
		}
		s.mu.Unlock()
	}
	return n, err
}

// current is the sending connection, nil once the set is closed.
func (s *relaySet) current() *relayConn {
	// Source of truth: https://github.com/Hellysonrp/meowcaller/blob/3c3a9e04ec5b78165c1cf5ceedbe7335ca304fab/datasheets/multi-relay.md#L33
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	return s.sending
}

// byEndpoint is the open connection to ep's address, nil when none is open.
func (s *relaySet) byEndpoint(ep *relayEndpoint) *relayConn {
	// Source of truth: https://github.com/Hellysonrp/meowcaller/blob/3c3a9e04ec5b78165c1cf5ceedbe7335ca304fab/datasheets/multi-relay.md#L35
	key := relayEndpointKey(ep)
	if key == "" {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, c := range s.conns {
		if relayEndpointKey(c.endpoint) == key {
			return c
		}
	}
	return nil
}

// pin makes c carry the call's media whatever arrives on other connections: a group call
// keeps its one connection, including a 1:1 call that became one.
func (s *relaySet) pin(c *relayConn) {
	// Source of truth: https://github.com/Hellysonrp/meowcaller/blob/3c3a9e04ec5b78165c1cf5ceedbe7335ca304fab/datasheets/multi-relay.md#L35
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closed && slices.Contains(s.conns, c) {
		s.sending, s.pinned = c, true
	}
}

// Recv reads the next packet any connection received, and the connection it came on.
// Once every connection has ended, its error is errRelaySetClosed joined with why the last
// one ended.
func (s *relaySet) Recv(buf []byte) (int, *relayConn, error) {
	// Source of truth: https://github.com/Hellysonrp/meowcaller/blob/3c3a9e04ec5b78165c1cf5ceedbe7335ca304fab/datasheets/multi-relay.md#L32-L34
	select {
	case p := <-s.packets:
		return copy(buf, p.data), p.conn, nil
	case <-s.done:
		select {
		case p := <-s.packets:
			return copy(buf, p.data), p.conn, nil
		default:
			s.mu.Lock()
			lastErr := s.lastErr
			s.mu.Unlock()
			return 0, nil, errors.Join(errRelaySetClosed, lastErr)
		}
	}
}

// follow moves the call's media to c, the connection the peer's audio arrived on, and
// reports whether it moved. A pinned connection is not moved.
func (s *relaySet) follow(c *relayConn) bool {
	// Source of truth: https://github.com/Hellysonrp/meowcaller/blob/3c3a9e04ec5b78165c1cf5ceedbe7335ca304fab/datasheets/multi-relay.md#L33
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.pinned || s.sending == c || !slices.Contains(s.conns, c) {
		return false
	}
	s.sending = c
	return true
}

// each runs fn for every open connection.
func (s *relaySet) each(fn func(*relayConn)) {
	// Source of truth: https://github.com/Hellysonrp/meowcaller/blob/3c3a9e04ec5b78165c1cf5ceedbe7335ca304fab/datasheets/multi-relay.md#L31
	s.mu.Lock()
	conns := slices.Clone(s.conns)
	s.mu.Unlock()
	for _, c := range conns {
		fn(c)
	}
}

// Close closes every connection; Recv returns an error from then on.
func (s *relaySet) Close() error {
	// Source of truth: https://github.com/Hellysonrp/meowcaller/blob/3c3a9e04ec5b78165c1cf5ceedbe7335ca304fab/datasheets/multi-relay.md#L34
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.closed = true
		conns := slices.Clone(s.conns)
		s.mu.Unlock()
		close(s.done)
		for _, c := range conns {
			_ = c.ch.Close()
		}
	})
	return nil
}

// relayDialAddresses is where a connection to ep dials: the offered address and the
// same IP at webClientRelayPort, once when they are the same.
func relayDialAddresses(ep *relayEndpoint) []*net.UDPAddr {
	// Source of truth: https://github.com/Hellysonrp/meowcaller/blob/3c3a9e04ec5b78165c1cf5ceedbe7335ca304fab/datasheets/multi-relay.md#L29
	if ep == nil || len(ep.addresses) == 0 {
		return nil
	}
	offered := ep.addresses[0]
	ip := net.ParseIP(offered.ipv4)
	addrs := []*net.UDPAddr{{IP: ip, Port: int(offered.port)}}
	if offered.port != webClientRelayPort {
		addrs = append(addrs, &net.UDPAddr{IP: ip, Port: webClientRelayPort})
	}
	return addrs
}

// dialFirst dials every address at once and keeps the first DataChannel to open; one
// that opens later is closed. ctx bounds the wait.
func (e *engine) dialFirst(ctx context.Context, addrs []*net.UDPAddr) (relayChannel, *net.UDPAddr, error) {
	// Source of truth: https://github.com/Hellysonrp/meowcaller/blob/3c3a9e04ec5b78165c1cf5ceedbe7335ca304fab/datasheets/multi-relay.md#L29
	if len(addrs) == 0 {
		return nil, nil, errors.New("meowcaller: relay has no address to dial")
	}
	type dialed struct {
		ch   *relay.RelayMediaChannel
		addr *net.UDPAddr
		err  error
	}
	results := make(chan dialed, len(addrs))
	for _, addr := range addrs {
		go func() {
			ch, err := relay.ConnectRelayMedia(addr, relay.WithLogger(e.c.log))
			results <- dialed{ch, addr, err}
		}()
	}
	// The dials left running when one wins, or when the wait ends, are drained in the
	// background so a late success does not leak its connection.
	closeLate := func(remaining int) {
		go func() {
			for range remaining {
				if r := <-results; r.err == nil {
					_ = r.ch.Close()
				}
			}
		}()
	}
	var errs []error
	for received := 0; received < len(addrs); received++ {
		select {
		case r := <-results:
			if r.err != nil {
				errs = append(errs, fmt.Errorf("%s: %w", r.addr, r.err))
				continue
			}
			closeLate(len(addrs) - received - 1)
			return r.ch, r.addr, nil
		case <-ctx.Done():
			closeLate(len(addrs) - received)
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return nil, nil, errors.New("relay connect timed out (DTLS didn't complete)")
			}
			return nil, nil, ctx.Err()
		}
	}
	return nil, nil, fmt.Errorf("relay connect: %w", errors.Join(errs...))
}

// openRelayConn opens one relay connection: it dials ep, sends its allocate (carrying ep
// as the offer lists it) and its consent ping.
func (e *engine) openRelayConn(ctx context.Context, rd *relayData, ep *relayEndpoint, streamSsrcs [9]uint32, hbhFEC [2]uint32) (*relayConn, error) {
	// Source of truth: https://github.com/Hellysonrp/meowcaller/blob/3c3a9e04ec5b78165c1cf5ceedbe7335ca304fab/datasheets/multi-relay.md#L29-L31
	log := e.c.log
	addrs := relayDialAddresses(ep)
	if addrs == nil {
		return nil, fmt.Errorf("relay %s has no address", ep.relayName)
	}
	if int(ep.tokenID) >= len(rd.relayTokens) || rd.relayTokens[ep.tokenID] == nil {
		return nil, fmt.Errorf("relay %s: no relay token #%d", ep.relayName, ep.tokenID)
	}
	if len(rd.relayKeyASCII) == 0 {
		return nil, errors.New("relay has no <key>")
	}
	endpointXor, ok := stun.EncodeXorRelayEndpoint(ep.addresses[0].ipv4, ep.addresses[0].port, log)
	if !ok {
		return nil, fmt.Errorf("relay %s: bad endpoint XOR", ep.relayName)
	}
	log.Info().Str("relay_name", ep.relayName).Str("addr", addrs[0].String()).Int("dials", len(addrs)).Msg("connecting media transport to relay")
	e.c.diag.Emit("relay", map[string]any{
		"event": "endpoint", "relay_name": ep.relayName,
		"ipv4": ep.addresses[0].ipv4, "port": ep.addresses[0].port, "token_id": ep.tokenID,
	})
	e.c.diag.Emit("relay", map[string]any{
		"event": "keying", "token_id": ep.tokenID, "token_count": len(rd.relayTokens),
		"relay_key_bytes": len(rd.relayKeyASCII),
		"token_bytes":     len(rd.relayTokens[ep.tokenID]),
	})
	ch, addr, err := e.dialFirst(ctx, addrs)
	if err != nil {
		return nil, fmt.Errorf("relay %s: %w", ep.relayName, err)
	}
	log.Info().Str("relay_name", ep.relayName).Str("addr", addr.String()).Msg("relay DataChannel open")

	var tx [12]byte
	_, _ = rand.Read(tx[:])
	allocate := stun.BuildWasmStunAllocateRequestWithStreamSsrcs(tx, rd.relayTokens[ep.tokenID], endpointXor, streamSsrcs, rd.relayKeyASCII, log)
	if _, err := ch.Send(allocate); err != nil {
		_ = ch.Close()
		return nil, fmt.Errorf("relay %s: allocate send: %w", ep.relayName, err)
	}
	log.Info().Str("relay_name", ep.relayName).Int("bytes", len(allocate)).Msg("sent STUN allocate")
	e.c.diag.Emit("stun", map[string]any{
		"event": "allocate_sent", "relay_name": ep.relayName, "bytes": len(allocate),
		"tx_id_hex": hex.EncodeToString(tx[:]), "stream_ssrcs": streamSsrcs,
	})
	// The consent ping goes with the allocate, before any RTP: the relay forwards nothing
	// before consent is established.
	var ptx [12]byte
	_, _ = rand.Read(ptx[:])
	ping := stun.BuildWhatsappPing(ptx, log)
	_, _ = ch.Send(ping[:])
	e.c.diag.Emit("stun", map[string]any{
		"event": "consent_ping_sent", "relay_name": ep.relayName,
		"tx_id_hex": hex.EncodeToString(ptx[:]), "ping_hex": hex.EncodeToString(ping[:]),
	})

	return &relayConn{
		name:     ep.relayName,
		endpoint: ep,
		ch:       ch,
		allocate: newGroupRelayAllocateStateWithHBHFEC(allocate, rd.relayKeyASCII, hbhFEC),
	}, nil
}

// connectRelays connects a 1:1 call to every relay the offer lists, returning as soon as
// one connection is open; the others join the set as they open, and one that fails is
// left out.
func (e *engine) connectRelays(ctx context.Context, rd *relayData, streamSsrcs [9]uint32, hbhFEC [2]uint32) (*relaySet, error) {
	// Source of truth: https://github.com/Hellysonrp/meowcaller/blob/3c3a9e04ec5b78165c1cf5ceedbe7335ca304fab/datasheets/multi-relay.md#L28-L30
	// NOT VALIDATED: validated once a live incoming call answered seconds after its offer carries audio both ways.
	targets := relayTargets(rd)
	if len(targets) == 0 {
		return nil, errors.New("relay has no usable endpoint")
	}

	set := newRelaySet()
	set.dialing(len(targets))
	opened := make(chan struct{}, len(targets))
	var dialErrsMu sync.Mutex
	var dialErrs []error
	timeout := relayConnectTimeout
	for _, ep := range targets {
		go func() {
			dialCtx, cancel := context.WithTimeout(ctx, timeout)
			c, err := e.openRelayConn(dialCtx, rd, ep, streamSsrcs, hbhFEC)
			cancel()
			if err != nil {
				e.c.log.Warn().Err(err).Str("relay_name", ep.relayName).Msg("relay connection failed; continuing without it")
				dialErrsMu.Lock()
				dialErrs = append(dialErrs, err)
				dialErrsMu.Unlock()
				set.dialFailed()
				return
			}
			set.add(c)
			opened <- struct{}{}
		}()
	}
	select {
	case <-opened:
		e.c.log.Info().Int("offered", len(targets)).Msg("first relay connection open; the others join as they open")
		return set, nil
	case <-set.done:
		dialErrsMu.Lock()
		defer dialErrsMu.Unlock()
		return nil, fmt.Errorf("no offered relay could be connected (%d offered): %w", len(targets), errors.Join(dialErrs...))
	case <-ctx.Done():
		_ = set.Close()
		return nil, ctx.Err()
	}
}

// relayTargets is every relay endpoint the offer lists, one per address: for two entries
// at one address, the FNA one, whose token the caller's uplink uses.
func relayTargets(rd *relayData) []*relayEndpoint {
	// Source of truth: https://github.com/Hellysonrp/meowcaller/blob/3c3a9e04ec5b78165c1cf5ceedbe7335ca304fab/datasheets/multi-relay.md#L28
	var targets []*relayEndpoint
	at := map[string]int{}
	for i := range rd.endpoints {
		ep := &rd.endpoints[i]
		key := relayEndpointKey(ep)
		if key == "" {
			continue
		}
		if j, ok := at[key]; ok {
			if ep.isFNA && !targets[j].isFNA {
				targets[j] = ep
			}
			continue
		}
		at[key] = len(targets)
		targets = append(targets, ep)
	}
	return targets
}

// relayEndpointKey is ep's offered address, empty when it has none.
func relayEndpointKey(ep *relayEndpoint) string {
	if ep == nil || len(ep.addresses) == 0 {
		return ""
	}
	return net.JoinHostPort(ep.addresses[0].ipv4, strconv.Itoa(int(ep.addresses[0].port)))
}

// maxDuplicateStreams bounds the SSRCs rtpDuplicates remembers: past it the stream marked
// least recently is forgotten.
const maxDuplicateStreams = 64

// rtpDuplicates remembers recent authenticated (SSRC, sequence) pairs, so a packet two
// relays both deliver is processed once.
type rtpDuplicates struct {
	streams map[uint32]*rtpSeenWindow
	order   []uint32 // SSRCs, least recently marked first
}

// rtpSeenWindow is one SSRC's recently seen sequence numbers.
type rtpSeenWindow struct {
	seq  [1024]uint16
	seen [1024]bool
}

func newRTPDuplicates() *rtpDuplicates {
	// Source of truth: https://github.com/Hellysonrp/meowcaller/blob/3c3a9e04ec5b78165c1cf5ceedbe7335ca304fab/datasheets/multi-relay.md#L32
	return &rtpDuplicates{streams: map[uint32]*rtpSeenWindow{}}
}

// has reports whether (ssrc, seq) was marked: a copy of a packet already processed.
func (d *rtpDuplicates) has(ssrc uint32, seq uint16) bool {
	// Source of truth: https://github.com/Hellysonrp/meowcaller/blob/3c3a9e04ec5b78165c1cf5ceedbe7335ca304fab/datasheets/multi-relay.md#L32
	window := d.streams[ssrc]
	if window == nil {
		return false
	}
	slot := int(seq) % len(window.seq)
	return window.seen[slot] && window.seq[slot] == seq
}

// mark records (ssrc, seq) once its packet authenticated, so a copy that fails to
// authenticate never blocks a valid one. Each SSRC keeps its last 1024 sequence numbers:
// over a minute of 60 ms audio.
func (d *rtpDuplicates) mark(ssrc uint32, seq uint16) {
	// Source of truth: https://github.com/Hellysonrp/meowcaller/blob/3c3a9e04ec5b78165c1cf5ceedbe7335ca304fab/datasheets/multi-relay.md#L32
	window := d.streams[ssrc]
	if window == nil {
		if len(d.order) == maxDuplicateStreams {
			delete(d.streams, d.order[0])
			d.order = d.order[1:]
		}
		window = &rtpSeenWindow{}
		d.streams[ssrc] = window
	} else {
		d.order = slices.DeleteFunc(d.order, func(s uint32) bool { return s == ssrc })
	}
	d.order = append(d.order, ssrc)
	slot := int(seq) % len(window.seq)
	window.seen[slot], window.seq[slot] = true, seq
}

// offeredRelayNames is the set of relay names the offer lists.
func offeredRelayNames(rd *relayData) map[string]bool {
	// Source of truth: https://github.com/Hellysonrp/meowcaller/blob/3c3a9e04ec5b78165c1cf5ceedbe7335ca304fab/datasheets/multi-relay.md#L36
	names := map[string]bool{}
	if rd == nil {
		return names
	}
	for _, ep := range rd.endpoints {
		if ep.relayName != "" {
			names[ep.relayName] = true
		}
	}
	return names
}
