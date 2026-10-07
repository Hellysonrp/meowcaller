# Datasheet: `engine/relay-timeout`

Ends a call's media when the relay has sent nothing for a configured time.

**Validation vector:** focused deterministic Go tests (the watchdog's expiry, its silence
while packets arrive, its exit on cancel, and the option's plumbing) plus a human-run
live call whose relay leg is cut.

**Reference pinned at:** `UNMAPPED` — the relay timeout is this fork's own design, not
a port. The human reviewer authorized this implementation on 2026-10-07.

## Reference source (verbatim — authoritative)

The following policy is human-authorized but remains live-E2E unvalidated:

```text
WithRelayTimeout(d) ends a call's media when nothing at all has arrived from the relay for d
while the leg is alive the relay answers the media loop's 1 Hz allocate and ping keepalive, so silence for d means the leg is gone
d of zero or less, the default, keeps the loop as it is: it ends only when a send or receive fails or its context ends
a media loop that times out returns ErrRelayTimeout, and RunMedia's Err reports it
the option applies wherever the media loop runs: a Client built with NewClient and a session run by RunMedia
```

## Go envelope (signatures only)

```go
package meowcaller

// config and Client gain: relayTimeout time.Duration

func WithRelayTimeout(d time.Duration) Option
var ErrRelayTimeout = errors.New("meowcaller: relay sent nothing within the relay timeout")
func watchRelay(ctx context.Context, rx *atomic.Uint64, timeout, tick time.Duration, expired func())
```

`NewClient` and `RunMedia` copy the timeout onto their `Client`; `runMedia` starts
`watchRelay` on its `relayRx` counter when the timeout is positive.

## Implementation suggestions (guidance, not authoritative)

- Tick once a second (the keepalive's period), or at the timeout when it is shorter.
- On expiry, close the relay channel: closing its SCTP association unblocks a waiting
  `ch.Recv` (pion/sctp v1.9.4: the association's read loop unregisters every stream with
  its close error). Close it once, whichever of the watchdog and the deferred close
  comes first.
