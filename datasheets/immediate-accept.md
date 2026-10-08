# Datasheet: `engine/immediate-accept`

Lets a client answer a 1:1 incoming call by sending its accept from `Answer` itself,
instead of deferring it to the caller's first `mute_v2`, so the accept can precede any
media the answering side sends.

**Validation vector:** focused deterministic Go tests (the accept sent from `Answer` with
`OnAcceptSent` reported, a later `mute_v2` sending nothing, a failed send returned from
`Answer`, the deferred accept kept without the option, and the option's plumbing) plus a
human-run live incoming call answered several seconds after its offer, with its media
connected after the accept, carrying audio both ways.

**Reference pinned at:** `UNMAPPED` — the immediate accept is this fork's own design,
not a port. The human reviewer authorized this implementation on 2026-10-07.

## Reference source (verbatim — authoritative)

The following policy is human-authorized but remains live-E2E unvalidated:

```text
a Client built with WithImmediateAccept answers a 1:1 incoming call by sending its accept at once, from Answer, instead of waiting for the caller's first mute_v2
a mute_v2 that arrives after that accept is an in-call mute-state change and sends nothing
a Client built without WithImmediateAccept keeps the deferred accept
OnAcceptSent fires when that accept went out, as it does for the deferred one
```

## Go envelope (signatures only)

```go
package meowcaller

// config and Client gain: immediateAccept bool
// engineCall gains: acceptSending bool — an immediate accept claimed and in flight

func WithImmediateAccept() Option
func (e *engine) answerAtOnce(c *Call) error
func (e *engine) transmitAccept(callID string, to, creator types.JID, video bool) error
```

`NewClient` copies the option onto its `Client`. With it, `answer` hands a 1:1 call to
`answerAtOnce`, which claims the accept under the engine lock, sends it addressed to the
offer's sender and creator (or to the early `mute_v2`'s, when one already arrived), and
marks the call answered only once it went out. A `mute_v2` meanwhile is recorded like
any early one and sends nothing. An accept that did not go out fails `Answer` and leaves
the call ringing, so a later `Answer` sends it again; once it went out, a repeated
`Answer` does nothing; an outgoing call, or an answer already in flight, is refused.
`transmitAccept` builds and sends the accept for both paths; the deferred path's
`sendAccept` keeps logging a failed send and returning nothing. A group accept is
unchanged.

## Implementation suggestions (guidance, not authoritative)

- Keep the accept's wire bytes as they are: only when it is sent changes.
- Never arm `acceptPending` on the immediate path: it is what lets a `mute_v2` send the
  deferred accept.
- Decide answered against the first inbound RTP under the engine lock, as `answer`
  does, so exactly one of them makes the call active.
