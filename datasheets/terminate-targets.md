# Datasheet: `engine/terminate-targets`

Ends an outgoing 1:1 call that is still ringing on every callee device the offer rang,
instead of on the bare peer only, so no callee device keeps ringing after a hangup.

**Validation vector:** focused deterministic Go tests (two rung devices and no accept:
two terminates, each to one device, with the call's creator, its own id and no
`<destination>`; one rung device: one terminate to that device; after an accept from
device 45: one terminate to it; after an accept from the primary device, whose JID is the
bare peer LID: one terminate to it and no fan-out; after an accept with an empty `from`:
one terminate per rung device; an incoming call and a group call: one terminate to
`from`, as before; the first send failing: the second still sent, the error
returned and the call ended locally) plus a human-run live outgoing call to a callee with the phone and
one more linked device, cancelled while it rings: every device stops ringing at once.
`placeCall`'s storing of the rung devices has no test seam, since it needs a live
connection; the live call covers it.

**Reference pinned at:** `e634d245f71d66e2ae55f79ef714c4ca01615ed8`

## Reference source (verbatim — authoritative excerpt)

`src/voip/facade.rs`, `place_call_prepared` (L2035-L2042):

```rust
    // The rung device set lives on the session so an inbound <accept>/<reject> from one callee device
    // can dismiss the rest (caller-driven accepted_elsewhere); it is dropped automatically whenever the
    // call deregisters (every end path removes the registry entry). Use the FULL server-rung set, NOT
    // the encrypted `device_keys` subset: a device we couldn't encrypt for (e.g. the primary phone,
    // dropped as pkmsg without an ADV account) still rings and must be dismissed, or it times the call
    // out. Retained for a single-device callee too: it has no sibling to dismiss, but it is still the
    // device a `<terminate>` has to reach if the call is cancelled before anyone answers.
    session.ring_devices = ring_devices;
```

`src/voip/facade.rs`, `CallHandle::terminate_targets` (L4113-L4139):

```rust
    /// Every address a `<terminate>` for this call must reach. One, except while an outgoing call is
    /// still ringing: call signaling is routed per device and the server does not fan a terminate
    /// out, so until one device answers, every device the offer rang has to be told.
    fn terminate_targets(&self) -> Vec<Jid> {
        if self
            .client_registry
            .group_state_if_current(&self.call_id, self.generation)
            .is_some()
        {
            return vec![Jid::new(&self.call_id, Server::Call)];
        }
        // One snapshot for both: read separately, an `<accept>` landing between them sets the
        // answerer and drains the rung set, leaving neither read holding a target.
        let Some(session) = self
            .client_registry
            .snapshot_if_current(&self.call_id, self.generation)
        else {
            return vec![self.peer_jid.clone()];
        };
        if let Some(device) = session.answering_device {
            return vec![device];
        }
        if !session.ring_devices.is_empty() {
            return session.ring_devices;
        }
        vec![self.peer_jid.clone()]
    }
```

`src/voip/facade.rs`, `CallHandle::terminate` (L5166-L5215):

```rust
    /// End the call: send `<terminate>` to the peer, then tear the local side down.
    ///
    /// A group or call-link call is addressed at the call scope. A direct call is addressed at the
    /// device that answered, or, while it is still ringing, at every device the offer rang, since the
    /// server does not fan a terminate out. The local teardown runs whether or not the stanzas went
    /// out, which is why this reports its outcome instead of returning an error the caller must
    /// handle to hang up.
    pub async fn terminate(&self) -> CallTermination {
        let client = match self.upgrade_host() {
            Ok(client) => client,
            Err(error) => {
                self.hangup_local().await;
                return CallTermination::LocalOnly(error);
            }
        };
        // Armed before the first await: waiting for the lane is cancellable too, and a caller that
        // drops this future must not be left with a live call. Generation-scoped, so a superseded
        // handle tears down nothing.
        let _teardown = DirectHandleTeardown {
            host: &*client,
            call_id: &self.call_id,
            generation: self.generation,
        };
        // The answer-transition lane, held across the check AND the send: a replacement installed in
        // that window would be ended by a `<terminate>` the peer cannot tell apart from ours, since
        // both carry the same call-id.
        let _transition = client.lock_answer_transition(&self.call_id).await;
        // Nothing of ours is live: a superseded handle must not signal, or the peer would drop the
        // replacement that now owns this call-id.
        if self.client_registry.generation_of(&self.call_id) != Some(self.generation) {
            return CallTermination::AlreadyEnded;
        }
        let targets = self.terminate_targets();
        let delivery = super::direct::terminate_for_generation(
            &*client,
            &self.call_id,
            &targets,
            &self.call_creator,
            self.generation,
        )
        .await;
        match delivery.failure {
            None => CallTermination::PeerNotified,
            Some(error) if delivery.notified == 0 => CallTermination::LocalOnly(error),
            Some(_) => CallTermination::PartlyNotified {
                notified: delivery.notified,
                unconfirmed: targets.len() - delivery.notified,
            },
        }
    }
```

`src/voip/direct_control.rs`, `terminate_for_generation` and `terminate_inner`
(L62-L117):

```rust
pub(crate) async fn terminate_for_generation(
    host: &dyn DirectCallHost,
    call_id: &str,
    peers: &[Jid],
    call_creator: &Jid,
    generation: u64,
) -> TerminateDelivery {
    let (notified, result) =
        terminate_inner(host, call_id, peers, call_creator, Some(generation)).await;
    TerminateDelivery {
        notified,
        failure: result.err(),
    }
}

pub(crate) async fn terminate_inner(
    host: &dyn DirectCallHost,
    call_id: &str,
    peers: &[Jid],
    call_creator: &Jid,
    generation: Option<u64>,
) -> (usize, Result<(), CallError>) {
    let epoch = host.call_state().epoch();
    let _teardown = generation.map(|generation| DirectTeardown {
        host,
        call_id,
        generation,
    });
    if call_id.is_empty() {
        return (0, Err(CallError::EmptyCallId));
    }
    let mut notified = 0;
    let mut failure = None;
    for peer in peers {
        if let Err(error) = host.call_state().ensure_epoch(epoch) {
            if failure.is_none() {
                failure = Some(error);
            }
            break;
        }
        let id = host.generate_request_id();
        let stanza = build_terminate(&TerminateParams {
            call_id,
            to: peer,
            id: Some(&id),
            call_creator,
            reason: None,
        });
        match host.send_node(stanza).await {
            Ok(()) => notified += 1,
            Err(error) if failure.is_none() => failure = Some(error),
            Err(_) => {}
        }
    }
    (notified, failure.map_or(Ok(()), Err))
}
```

`src/voip/direct_ingress.rs`, the inbound `<accept>` recording the answering device
(L319-L327):

```rust
    if let CallAction::Accept { .. } = &call.action {
        let sender = routed_call_sender(call);
        // Record the device that answered so a later <terminate> targets it (call
        // signaling is addressed per device, not to the bare peer the offer rang).
        if direct_accept.is_none() {
            client
                .call_registry()
                .set_answering_device(call.action.call_id(), sender.clone());
        }
```

`wacore/src/voip_control/registry.rs`, `set_answering_device` (L3205-L3215):

```rust
    /// Caller side: record the callee device that answered (the inbound `<accept>`'s `call.from`), so a
    /// later `<terminate>` can target it instead of the bare peer the offer rang. Set-once -- the first
    /// answerer wins, matching the rekey and WA Web's accepted-elsewhere handling. No-op if the call is
    /// unknown or a device was already recorded.
    pub fn set_answering_device(&self, call_id: &str, device: Jid) {
        if let Some(entry) = self.active_calls().get_mut(call_id)
            && entry.session.answering_device.is_none()
        {
            entry.session.answering_device = Some(device);
        }
    }
```

`src/voip/facade.rs`, test `terminate_signals_the_answering_device` (L8580-L8626):

```rust
    // The reported symptom: ending a call from the handle used to be local-only, so the peer kept
    // talking until its transport timed out. terminate() must put exactly one <terminate> on the wire,
    // addressed at the device that answered the <accept>, and tear the local call down.
    #[tokio::test]
    async fn terminate_signals_the_answering_device() {
        let (client, sends) = make_sending_client().await;
        let registry = client.call_registry();
        let generation = registry.insert(mk_session());
        let device = caller().with_device(3);
        registry.set_answering_device("CID-FACADE", device.clone());
        let handle = registry_handle(&client, generation);

        let waiter = client.wait_for_sent_node(crate::client::NodeFilter::tag("call"));
        let outcome = handle.terminate().await;
        assert!(
            outcome.peer_notified(),
            "a reachable peer must be told: {outcome:?}"
        );

        let node = tokio::time::timeout(Duration::from_secs(2), waiter)
            .await
            .expect("terminate must be sent")
            .expect("waiter");
        let wrapper = node.as_node_ref();
        assert_eq!(
            wrapper.attrs().optional_jid("to"),
            Some(device),
            "the terminate must reach the device that answered, not the bare peer"
        );
        let action = &wrapper.children().expect("call action")[0];
        assert_eq!(action.tag.as_ref(), "terminate");
        assert_eq!(
            action.attrs().optional_string("call-id").as_deref(),
            Some("CID-FACADE")
        );
        assert_eq!(action.attrs().optional_jid("call-creator"), Some(caller()));
        assert_eq!(
            sends.load(Ordering::SeqCst),
            1,
            "exactly one terminate, not one per teardown step"
        );
        assert_eq!(
            registry.active_count(),
            0,
            "signaling the peer must still end our own call"
        );
    }
```

`src/voip/facade.rs`, test `partly_delivered_terminate_reports_what_reached_the_wire`
(L8889-L8920):

```rust
    // A still-ringing call told some of its devices and not others is not the same as telling nobody:
    // the outcome has to say which, or a consumer logs a silent hangup that was not silent.
    #[tokio::test]
    async fn partly_delivered_terminate_reports_what_reached_the_wire() {
        // The second send fails, so the first device is told and the second is not.
        let (client, sends) = make_sending_client_with_failure_after(Some(1)).await;
        let mut session =
            wacore::voip_control::CallSession::new_outgoing("CID-FACADE", caller(), caller());
        session.ring_devices = vec![caller().with_device(1), caller().with_device(2)];
        let generation = client.call_registry().insert(session);
        let handle = registry_handle(&client, generation);

        let outcome = handle.terminate().await;

        assert!(
            matches!(
                outcome,
                CallTermination::PartlyNotified {
                    notified: 1,
                    unconfirmed: 1
                }
            ),
            "a partial fan-out must be reported as such: {outcome:?}"
        );
        assert!(!outcome.peer_notified(), "not every device was confirmed");
        assert_eq!(sends.load(Ordering::SeqCst), 2, "both sends were attempted");
        assert_eq!(
            client.call_registry().active_count(),
            0,
            "the local call ends either way"
        );
    }
```

`src/voip/facade.rs`, test `terminate_while_ringing_reaches_every_rung_device`
(L9177-L9215):

```rust
    // An outgoing call still ringing has no answering device yet, and the server does not fan a
    // terminate out: cancelling it must reach every device the offer rang, or the rest keep ringing.
    #[tokio::test]
    async fn terminate_while_ringing_reaches_every_rung_device() {
        let (client, sends) = make_sending_client().await;
        let first = caller().with_device(1);
        let second = caller().with_device(2);
        let mut session =
            wacore::voip_control::CallSession::new_outgoing("CID-FACADE", caller(), caller());
        session.ring_devices = vec![first.clone(), second.clone()];
        let generation = client.call_registry().insert(session);
        let handle = registry_handle(&client, generation);

        let to_first = client.wait_for_sent_node(
            crate::client::NodeFilter::tag("call").attr("to", first.to_string()),
        );
        let to_second = client.wait_for_sent_node(
            crate::client::NodeFilter::tag("call").attr("to", second.to_string()),
        );
        assert!(handle.terminate().await.peer_notified());

        for (waiter, device) in [(to_first, first), (to_second, second)] {
            let node = tokio::time::timeout(Duration::from_secs(2), waiter)
                .await
                .unwrap_or_else(|_| panic!("{device} must be told"))
                .expect("waiter");
            assert_eq!(
                node.as_node_ref().children().expect("call action")[0]
                    .tag
                    .as_ref(),
                "terminate"
            );
        }
        assert_eq!(
            sends.load(Ordering::SeqCst),
            2,
            "one stanza per rung device"
        );
    }
```

`src/voip/facade.rs`, test `terminate_while_ringing_reaches_a_lone_device`
(L9217-L9258):

```rust
    // A single-device callee is still a device: cancelling before it answers must reach that JID, the
    // one the offer resolved, not the bare peer.
    #[tokio::test]
    async fn terminate_while_ringing_reaches_a_lone_device() {
        let (client, sends) = make_sending_client().await;
        let device = peer_lid();
        seed_peer_session(&client, &device).await;
        let own_lid = client.lid().expect("own lid");
        let (_mic_tx, mic_rx) = async_channel::unbounded::<Vec<i16>>();
        let (spk_tx, _spk_rx) = async_channel::unbounded::<Vec<i16>>();
        let handle = place_call(
            &client,
            "00abcdef0123456789abcdef01234567".into(),
            &Jid::new("333333333333333", Server::Lid),
            &own_lid,
            &own_lid,
            std::slice::from_ref(&device),
            std::slice::from_ref(&device),
            pcm_audio(Arc::new(mic_rx), Arc::new(spk_tx)),
            None,
        )
        .await
        .expect("place_call");
        let offers = sends.load(Ordering::SeqCst);

        let waiter = client.wait_for_sent_node(
            crate::client::NodeFilter::tag("call").attr("to", device.to_string()),
        );
        assert!(handle.terminate().await.peer_notified());

        let node = tokio::time::timeout(Duration::from_secs(2), waiter)
            .await
            .expect("the lone device must be told")
            .expect("waiter");
        assert_eq!(
            node.as_node_ref().children().expect("call action")[0]
                .tag
                .as_ref(),
            "terminate"
        );
        assert_eq!(sends.load(Ordering::SeqCst), offers + 1);
    }
```

The following adaptation is human-authorized (approved on 2026-11-04) but remains
live-E2E unvalidated:

```text
placeCall keeps every device GetUserDevices returned for the peer, set under the engine lock in the same block as from and creator
that list is exactly the devices the offer rang: placeCall fails when the call key cannot be encrypted for any one device, so unlike the reference there is no rung device without a key
an accept of an outgoing 1:1 call marks the call answered in the same engine-lock block, and under the same condition, that sets from to the accept's from: only when that from is not empty, so answered means from names the device that answered
when the primary device answers, its JID can equal the bare peer LID placeCall stored, so from does not change; the answered mark, not a change of from, is what stops the fan-out
an accept with an empty from leaves the call unanswered, so a hangup still reaches every rung device, the one that answered among them
hangup reads the call's fields in one snapshot taken under the engine lock
an outgoing 1:1 call that no device has answered, with stored devices, gets one terminate per stored device: to is that device, call-creator is the call's creator, no reason, no destination, each with its own stanza id
every other case gets one terminate to from, as before; without a call entry, to and call-creator stay the Call's peer, as before
the local teardown runs before the sends, whatever they return
hangup sends to every target even when one send fails, and returns the first send error, where the reference reports a delivery outcome
the reference's epoch, generation and answer-transition guards have no counterpart: the local teardown removes the call entry before any send
no device list leaves the engine: no public API and no MediaSession change
dismissing the other rung devices when one accepts or rejects (the reference's caller-driven accepted_elsewhere) is not part of this change
```

## Go envelope (signatures only)

```go
package meowcaller

// engineCall gains: ringDevices []types.JID — the devices an outgoing 1:1 offer rang
//                   peerAnswered bool — from names the callee device that accepted this outgoing 1:1 call

// terminateTargets returns, from one snapshot under the engine lock, the JIDs a hangup of
// c sends a terminate to and the call-creator they carry.
func (e *engine) terminateTargets(c *Call) (targets []types.JID, creator types.JID)
```

`placeCall` sets `ringDevices` in its engine-lock block. `onAccept`, for a 1:1 call, sets
`peerAnswered` where it sets `from`, only when `ev.From` is not empty. `hangup` takes `terminateTargets`, runs `finishCall`, then sends one terminate per
target, each built with `signaling.BuildTerminate` and given its own `nextCallNodeID`
stanza id, and returns the first send error wrapped as `send terminate: %w`.
`datasheets/media-handoff.md`'s line that the address and the creator are the JIDs Hangup
and Reject address their stanzas with gains the exception: while an outgoing call is
still ringing, Hangup addresses each device the offer rang.

## Implementation suggestions (guidance, not authoritative)

- `terminateTargets`: no entry gives `[c.peer]` and `c.peer`; a group call, an incoming
  call, an answered call or an empty device list gives `[m.from]` and `m.creator`;
  otherwise a copy of `ringDevices` and `m.creator`.
- Store a copy of the device slice, and hand `hangup` a copy, so no caller can alias the
  entry's list.
- Log each failed send at warn with `call_id` and `to`, since only the first error is
  returned; log the target count at debug.
- Seed the tests like `testEngineWithOutgoingCall` and capture `sendCallNode`; use
  `testEngineWithIncomingCall` and `testGroupEngine` for the unchanged cases. The
  primary-device case runs `onAccept` from `peerJID()`, the seed's own `from`, over two
  stored devices.
- Build the tests' JIDs from made-up numbers (`peerJID`, `creatorJID`, `peerDevice`),
  never from a real call's log.
