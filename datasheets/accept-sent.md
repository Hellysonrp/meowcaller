# Datasheet: `engine/accept-sent`

Reports, once, when this client's accept for an incoming call has gone out.

**Validation vector:** focused deterministic Go tests (accept at Answer after an early
mute_v2, accept at the first mute_v2 after Answer, a failed send, a late listener, a group accept).

**Reference pinned at:** `UNMAPPED` — the accept report is this fork's own design,
not a port. The human reviewer authorized this implementation on
2026-10-07.

## Reference source (verbatim — authoritative)

The following policy is human-authorized:

```text
a Call reports, once, that this client's accept for an incoming call has been sent
sent means the accept stanza went out without a send error; nothing is reported for a failed send, which stays logged only
a 1:1 call's accept is sent by sendAccept: at Answer when a mute_v2 was already seen while ringing, otherwise at the first mute_v2 after Answer
a group call's accept is sent by Answer itself
a listener registered after the accept went out is called at once, as OnPeerAccept does
it behaves the same with or without a media handoff
```

## Go envelope (signatures only)

```go
package meowcaller

// Call gains: onAcceptSent func(); acceptSent bool; acceptSentNotified bool
// engine gains: newRequestID func() string

func (c *Call) OnAcceptSent(fn func())
func (c *Call) markAcceptSent()
```

`newEngine` sets `newRequestID` when a WhatsApp client exists; `sendAccept` sends through
`transmitCallNode` and calls `markAcceptSent` after a send without error; `answer` calls
it after a group accept is sent.

## Implementation suggestions (guidance, not authoritative)

- Mirror `OnPeerAccept` and `markPeerAccepted`: flags and callback under `c.mu`, the
  callback outside it, at most once.
- Keep the accept's wire bytes and stanza id unchanged; only the send path moves to
  `transmitCallNode` so a test can stand in for it.
