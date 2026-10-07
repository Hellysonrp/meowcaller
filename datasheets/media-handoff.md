# Datasheet: `engine/media-handoff`

Hands a 1:1 call's media session from the client that holds the WhatsApp connection
to a media runner elsewhere, and runs a handed-off session's media with no WhatsApp
client.

**Validation vector:** focused deterministic Go tests (handoff start, peer-change
forwarding, group refusal, relay conversion, `RunMedia` construction, stop and rekey)
plus a human-run live 1:1 call with its media on a different host from its signaling.

**Reference pinned at:** `UNMAPPED` — the handoff is this fork's own design, not a
port. The human reviewer authorized this implementation
on 2026-10-07.

## Reference source (verbatim — authoritative)

The handoff is this fork's own design. The engine's own media start sets the
constraints:

```text
media starts once per call, when both the call key and the relay allocation are known
an incoming call starts media at the offer; an outgoing call when the offer's ack carries the relay
a peer change after media start reaches the media loop through the call's rekeyPeer hook
runMedia installs rekeyPeer after the relay connects, then rekeys to the call's current peerLID
for a 1:1 call runMedia reads the call key, relay key, relay tokens, relay endpoints, self LID, peer LID and direction
```

The following policy is human-authorized but remains live-E2E unvalidated:

```text
a client built with WithMediaHandoff never runs media itself
when it would start media for a 1:1 call, it calls StartMedia once with the call's session, after releasing the engine lock
it sets the call's rekeyPeer to forward every later peer change to PeerChanged
a group call under a handoff starts no media
signaling is unchanged: Answer, Reject, Hangup, OnEnd, and the Ringing, Calling and Connecting phases
Active and OnReady never fire on the signaling side under a handoff
the session is plain data: call ID, call key, self and peer LIDs, direction, codec, relay key, relay tokens and relay endpoints
the session leaves out the relay's peer JID, video state and app-data state, and owns copies of every byte slice
RunMedia validates the session, then runs it on an engine with no WhatsApp client
RunMedia's call starts Connecting and is marked answered, so its first inbound audio fires OnReady
Play, Subscribe, Receive and OnReady act on RunMedia's call as on any call
Rekey stores the new peer, and calls rekeyPeer once the loop has installed it
when the media loop ends, the call is finished and Done closes; Err is the loop's error
Stop cancels the media loop
```

## Go envelope (signatures only)

```go
package meowcaller

type MediaHandoff interface {
	StartMedia(session MediaSession)
	PeerChanged(callID, peerLID string)
}

type MediaSession struct {
	CallID    string
	CallKey   []byte
	SelfLID   string
	PeerLID   string
	Direction CallDirection
	Codec     AudioCodec
	Relay     MediaSessionRelay
}

type MediaSessionRelay struct {
	Key       []byte
	Tokens    [][]byte
	Endpoints []MediaSessionEndpoint
}

type MediaSessionEndpoint struct {
	RelayID     uint32
	RelayName   string
	TokenID     uint32
	AuthTokenID uint32
	IsFNA       bool
	Addresses   []MediaSessionAddress
}

type MediaSessionAddress struct {
	IPv4 string
	Port uint16
}

type MediaCall struct {
	// Internal engine, call, cancel func, done channel and loop error.
}

func WithMediaHandoff(h MediaHandoff) Option
func newMediaSessionRelay(rd *relayData) MediaSessionRelay
func (r MediaSessionRelay) relayData() *relayData
func cloneByteSlices(in [][]byte) [][]byte
func mediaSessionLocked(callID string, m *engineCall) MediaSession
func (e *engine) prepareHandoffLocked(callID string, m *engineCall, h MediaHandoff) func()
func RunMedia(ctx context.Context, session MediaSession, opts ...Option) (*MediaCall, error)
func (c *MediaCall) Play(src AudioSource) *Player
func (c *MediaCall) Subscribe(p *Player)
func (c *MediaCall) Receive(sink AudioSink)
func (c *MediaCall) OnReady(fn func())
func (c *MediaCall) Rekey(peerLID string) error
func (c *MediaCall) Stop()
func (c *MediaCall) Done() <-chan struct{}
func (c *MediaCall) Err() error
```

`config` and `Client` each gain a `mediaHandoff MediaHandoff` field; `NewClient`
copies it, and `maybeStartMedia` hands off before it would launch `runMedia`.

## Implementation suggestions (guidance, not authoritative)

- Copy byte slices across the boundary, so neither side can change the other's key
  material.
- Never log the call key, relay key or tokens; log the endpoint count.
- Build the session under the engine lock and call `StartMedia` after releasing it.
- Keep `RunMedia`'s engine free of a WhatsApp client: `newEngine` already tolerates
  a nil one.
