# Datasheet: `engine/outgoing-reject`

Keeps an outgoing 1:1 call ringing when a callee device that cannot take the call
rejects it.

**Validation vector:** focused deterministic Go tests (secondary-device reject before
and after a preaccept, primary-device reject, preaccepted-device reject, incoming
reject) plus a human-run live outgoing call to a callee with WhatsApp Web linked.

**Reference pinned at:** `UNMAPPED` — the failure below is upstream issue #25 and
the policy is this fork's own. The human reviewer authorized this
implementation on 2026-10-07.

## Reference source (verbatim — authoritative)

The observed failure (upstream issue #25):

```text
placeCall offers the call to every device GetUserDevices returns for the peer
a linked WhatsApp Web or Desktop device that cannot take it answers with a reject
onReject ends the call on any reject from any device, while the phone still rings
```

The following policy is human-authorized but remains live-E2E unvalidated:

```text
an outgoing 1:1 call ends on a reject only from device 0 or from a device that preaccepted it
any other device's reject is logged and ignored, and the call keeps ringing
every preaccepting device is remembered, since several devices can ring at once
incoming calls and group calls end on any reject, as before
```

## Go envelope (signatures only)

```go
package meowcaller

// engineCall gains: preAccepted map[types.JID]struct{}

func (e *engine) recordPreAccept(callID string, from types.JID)
func (e *engine) ignoresSecondaryReject(callID string, from types.JID) bool
```

`onPreAccept` records every preaccepting device; `onReject` returns early when
`ignoresSecondaryReject` says so.

## Implementation suggestions (guidance, not authoritative)

- Record `ev.From` in `onPreAccept`, which already handles outgoing calls only, and
  skip an empty JID.
- Compare whole JIDs, device included.
- Read the set under the engine lock; log an ignored reject with `call_id` and
  `from`.
