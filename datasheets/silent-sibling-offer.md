# Datasheet: `engine/silent-sibling-offer`

Ignores the silent offer WhatsApp sends to this account's other linked devices when a
call this account is already connected to turns multi-party, so a sibling device that
is not in the call never answers it and the call keeps running on the device that is.

**Validation vector:** focused deterministic Go tests (the `<silence>` child parsed with
and without a reason; this account found connected by its LID with a device part and by
the roster's `user_pn`, and not found when it is absent, not connected, or its own JID is
empty; an offer shaped like the observed one dropped with nothing sent, no call entry and
no `OnIncomingCall`; the same offer without `<silence>`, and a silent offer whose roster
does not show this account connected, preaccepted and handed to `OnIncomingCall` as
before) plus a human-run live call on an account with two linked devices served by
meowcaller: a participant added mid-call no longer ends the call.

**Reference pinned at:** `UNMAPPED` — the failure below was observed on a live call and
the policy is this fork's own. The human reviewer authorized this implementation on
2026-10-21.

## Reference source (verbatim — authoritative)

The observed failure (an account with two linked devices, A and B, each with its own
`Client`):

```text
a 1:1 call is answered on B; A receives terminate reason="accepted_elsewhere"
the peer adds a participant; A receives an offer with joinable="1" whose group_info lists this account's user state="connected" (its device B) and which carries <silence reason="added_for_device_switching"/>
onOffer hands it to onGroupOffer, which preaccepts it and fires OnIncomingCall; the integrator rejects it, and the reject goes to <call-id>@call
B receives terminate reason="rejected_elsewhere" and the live call ends
```

The following policy is human-authorized but remains live-E2E unvalidated:

```text
an inbound offer that carries a <silence> child and whose group_info roster shows this account's user in state "connected" is dropped
a dropped offer sends no preaccept and no reject, creates no call entry and fires no OnIncomingCall; one log line says why
this account's user is the client's own LID or phone-number JID; a roster user is this account when its jid or its user_pn equals one of them, both compared without the device part
every other offer is handled as before: a 1:1 offer, a silent offer whose roster does not show this account connected, and an offer without <silence>
```

## Go envelope (signatures only)

```go
package signaling

// OfferSilence reports the reason of an inbound <offer>'s <silence> child, and whether
// it has one.
func OfferSilence(offer *waBinary.Node) (reason string, silenced bool)
```

```go
package meowcaller

// ownAccountConnected reports whether participants shows one of self's users in state
// "connected".
func ownAccountConnected(participants []signaling.GroupCallParticipant, self []types.JID) bool
```

`onOffer`, on a group offer, returns before `onGroupOffer` when `OfferSilence` reports
the offer silenced and `ownAccountConnected(groupSnapshot.Participants,
e.c.groupSelfJIDs())` holds. `groupSelfJIDs` already gives the client's phone-number JID
and LID from the store. whatsmeow acks the `<call>` itself, so the drop leaves the ack
as it is.

## Implementation suggestions (guidance, not authoritative)

- Compare `ToNonAD()` on both sides, and skip an empty JID on either side, so a roster
  user without `user_pn` never matches a client whose phone-number JID is empty.
- A `<silence>` child without a `reason` still counts as silenced.
- Log the drop at info with `call_id` and `silence_reason`: "ignoring silent offer for a
  call this account is already connected to".
- Build the tests' JIDs from made-up numbers, never from a real call's log.
