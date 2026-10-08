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

func WithImmediateAccept() Option
func (e *engine) sendAccept(callID string, to, creator types.JID) error // was: no result
```

`NewClient` copies the option onto its `Client`. With it, `answer` sends a 1:1 accept
at once, addressed to the offer's sender and creator (or to the early `mute_v2`'s, when
one already arrived), and returns the send's error: an immediate accept that did not go
out fails `Answer`. The deferred path keeps logging a failed send and returning nothing
from `Answer`. A group accept is unchanged.

## Implementation suggestions (guidance, not authoritative)

- Keep the accept's wire bytes as they are: only when it is sent changes.
- `sendAccept` already clears `acceptPending`, which is what makes a later `mute_v2`
  an in-call mute-state change; the immediate path needs nothing more for it.
