# Datasheet: `engine/sending-held`

Runs a handed-off call's media with its sending held until the caller of `RunMedia`
releases it, so the relay leg can come up as soon as the session exists while nothing
reaches the peer before the call is answered.

**Validation vector:** focused deterministic Go tests against a loopback relay (no RTP
or SRTCP while held, media flowing after `StartSending`, a repeated `StartSending`, and
media run without the option) plus a human-run live incoming call answered several
seconds after its offer, with audio both ways.

**Reference pinned at:** `UNMAPPED` — holding the sending is this fork's own design,
not a port. The human reviewer authorized this implementation on 2026-10-07.

## Reference source (verbatim — authoritative)

The following policy is human-authorized but remains live-E2E unvalidated:

```text
RunMedia with WithSendingHeld runs a call's media with its sending held: it connects to the relay, sends the allocate, keeps the leg alive (allocate, ping and binding responses) and receives as usual, but sends no RTP, video, app data or SRTCP
MediaCall.StartSending ends the hold, once: sending starts with the next frame; a repeated call does nothing
media run without WithSendingHeld sends as before
```

## Go envelope (signatures only)

```go
package meowcaller

// config gains: sendingHeld bool
// engineCall gains: sendHeld *atomic.Bool — set by RunMedia when its options hold the
// sending, nil otherwise

func WithSendingHeld() Option
func (c *MediaCall) StartSending()
func sendingIsHeld(held *atomic.Bool) bool
```

`RunMedia` sets `engineCall.sendHeld`, holding, when its options carry `WithSendingHeld`;
`runMedia` reads it once at its start. While it holds, these sends are skipped: the audio
send loop's RTP, the periodic audio and video SRTCP reports, the video PLI feedback, the
video sender, and the app-data sender. These sends are never held: the allocate (the
initial one, the keepalive, and a group allocation), the initial and keepalive pings,
and the binding-success responses.

## Implementation suggestions (guidance, not authoritative)

- Skip a held audio frame before it is encoded and protected, so the RTP sequence,
  timestamp and sender statistics start with the first frame actually sent.
- Skip a whole SRTCP tick while held: a sender report for a stream with nothing sent
  carries no information the peer needs.
- `StartSending` stores false; a second store changes nothing.
- `NewClient` ignores the option: a client's own media is never handed off, so it has
  no `MediaCall` to release it.
