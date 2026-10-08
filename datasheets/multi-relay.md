# Datasheet: `engine/multi-relay`

Connects a 1:1 call's media to every relay the offer lists, receives on all of them,
sends on the one the peer's audio arrives on, and limits the callee's relay-latency
answers to those relays, so the relay the caller elects is one the call is on.

**Validation vector:** focused deterministic Go tests against loopback relays (every
offered relay connected, the media starting on the first to open, a relay that never
opens left out, packets merged from every connection with duplicates dropped, sending
moving to the connection the peer's authenticated audio arrives on, the relay timeout
and the end over every connection, the port race, a group call's single connection, and
the relay-latency answers limited to the offer) plus a human-run live incoming call
answered seconds after its offer, with audio both ways.

**Reference pinned at:** `UNMAPPED` — this is the fork's own design, not a port. It
follows what WhatsApp Web's own `WAWebVoipSctpConnectionManager` does: it opens a
connection to every relay in the call's relay list, dials `TRUE_WEB_CLIENT_RELAY_PORT`
(3480) rather than the offered `FAUX_WEB_CLIENT_RELAY_PORT` (3478) unless a remote flag
says otherwise, keeps each relay's original port for the stack that builds its packets,
and sends each packet to the one relay its native stack names. The human reviewer
authorized this implementation on 2026-10-08.

## Reference source (verbatim — authoritative)

The following policy is human-authorized but remains live-E2E unvalidated:

```text
for a 1:1 call, the media connects to every relay endpoint the offer lists, each over its own DTLS/SCTP DataChannel, all at once
each connection dials the endpoint's offered port and 3480 together and keeps whichever completes its handshake first; its allocate carries the endpoint as the offer lists it
the media starts as soon as one connection is open; the others join as they open, and one that fails to open is left out
each connection sends its own allocate and consent ping when it opens, its own allocate and ping every second, and answers the binding requests it receives
packets received on any connection feed the call's one media pipeline; an RTP packet already received on another connection is dropped
the call's media is sent on one connection at a time: the first one to open, until authenticated audio from the peer arrives on another connection, which then carries it
the relay timeout counts what arrives on every connection, and the media ends when every connection has closed
a group call keeps its single relay connection
a callee answers the caller's relaylatency probes only for relays the offer lists; a probe for any other relay gets no answer
```

## Go envelope (signatures only)

```go
package meowcaller

// webClientRelayPort is the port WhatsApp Web dials on every relay.
const webClientRelayPort = 3480

// relayConn is one relay connection of a call: its DataChannel and its own allocate.
type relayConn struct {
	name     string
	ch       *relay.RelayMediaChannel
	allocate *groupRelayAllocateState
}

// relaySet is a call's relay connections.
type relaySet struct{ /* connections, the sending one, merged inbound, pending dials */ }

func newRelaySet() *relaySet
func (s *relaySet) add(c *relayConn)                         // a connection that opened joins
func (s *relaySet) dialing(n int)                            // n dials still pending
func (s *relaySet) dialFailed()                              // one pending dial gave up
func (s *relaySet) Send(packet []byte) (int, error)          // on the sending connection
func (s *relaySet) Recv(buf []byte) (int, *relayConn, error) // from any connection
func (s *relaySet) follow(c *relayConn)                      // the peer's audio arrived on c
func (s *relaySet) each(fn func(*relayConn))                 // every open connection
func (s *relaySet) Close() error

func (e *engine) dialRelay(ctx context.Context, ep *relayEndpoint) (*relay.RelayMediaChannel, error)
func (e *engine) openRelayConn(ctx context.Context, rd *relayData, ep *relayEndpoint, streamSsrcs [9]uint32, hbhFEC [2]uint32) (*relayConn, error)
func (e *engine) connectRelays(ctx context.Context, rd *relayData, streamSsrcs [9]uint32, hbhFEC [2]uint32) (*relaySet, error)

// rtpDuplicates remembers recent (SSRC, sequence) pairs.
type rtpDuplicates struct{ /* per-SSRC window */ }
func newRTPDuplicates() *rtpDuplicates
func (d *rtpDuplicates) seen(ssrc uint32, seq uint16) bool

func offeredRelayNames(rd *relayData) map[string]bool
```

`runMedia` uses a `relaySet` for every call: a 1:1 call's from `connectRelays`, a group
call's holding the one connection `connectAndAllocate` opens today, unchanged. Every
media send (audio RTP, SRTCP reports and PLI, video, app data) goes through
`relaySet.Send`; the allocate, the pings and the binding-success answers go on their own
connection. The receive loop reads `relaySet.Recv`, drops an RTP packet `rtpDuplicates`
has seen, and calls `follow` with the connection of each authenticated peer audio
packet. `onRelayLatency` answers only the probes whose `relay_name` is in
`offeredRelayNames(m.relay)`.

## Implementation suggestions (guidance, not authoritative)

- `dialRelay` runs `relay.ConnectRelayMedia` on the offered address and on the same IP
  at 3480 at once (one dial when the offered port already is 3480), keeps the first to
  succeed, closes the other if it succeeds later, and gives up after the 12 s connect
  timeout `connectAndAllocate` uses.
- `connectRelays` dials every endpoint the offer lists, deduplicated by address, returns
  once the first connection is open, and adds the others to the set from their own
  goroutines; it fails only when every dial failed.
- `Recv` returns an error once no connection is open and no dial is pending.
- Keep `relayRx` counting in the receive loop, which now sees every connection.
