package meowcaller

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"go.mau.fi/whatsmeow/types"
)

// MediaCall is a 1:1 call whose media runs here while its signaling runs on another
// Client, which handed its session over through MediaHandoff.
type MediaCall struct {
	eng    *engine
	call   *Call
	cancel context.CancelFunc
	done   chan struct{}
	err    error
	held   *atomic.Bool
}

// RunMedia starts a handed-off session's media with no WhatsApp client: the relay
// connection, the codec and the end-to-end SRTP. Done reports when it ends.
func RunMedia(ctx context.Context, session MediaSession, opts ...Option) (*MediaCall, error) {
	// Source of truth: https://github.com/Hellysonrp/meowcaller/blob/9259460582560c1dcc6da66ec9c94abc15c71b70/datasheets/media-handoff.md#L39-L40
	// NOT VALIDATED: validated once a live 1:1 call carries audio both ways with its media on a different host and public IP from its signaling.
	switch {
	case session.CallID == "":
		return nil, errors.New("meowcaller: media session has no call ID")
	case len(session.CallKey) == 0:
		return nil, errors.New("meowcaller: media session has no call key")
	case session.PeerLID == "":
		return nil, errors.New("meowcaller: media session has no peer LID")
	case len(session.Relay.Endpoints) == 0:
		return nil, errors.New("meowcaller: media session has no relay endpoint")
	}
	peer, err := types.ParseJID(session.PeerLID)
	if err != nil {
		return nil, fmt.Errorf("meowcaller: parse media session peer LID: %w", err)
	}

	cfg := resolveConfig(opts)
	// Source of truth: https://github.com/Hellysonrp/meowcaller/blob/c94a2d54a5179636eff02b57575f2ee8e8372cf6/datasheets/relay-timeout.md#L21
	c := &Client{log: cfg.log, diag: cfg.diag, relayTimeout: cfg.relayTimeout}
	e := newEngine(c)
	c.eng = e
	call := &Call{eng: e, id: session.CallID, peer: peer, phase: CallPhaseConnecting}
	rd := session.Relay.relayData()
	mctx, cancel := context.WithCancel(ctx)
	// Source of truth: https://github.com/Hellysonrp/meowcaller/blob/3d49711d486ddb54af1c918043f93940061787bb/datasheets/sending-held.md#L20
	var held *atomic.Bool
	if cfg.sendingHeld {
		held = &atomic.Bool{}
		held.Store(true)
	}
	e.calls[session.CallID] = &engineCall{
		call:      call,
		callKey:   bytes.Clone(session.CallKey),
		relay:     rd,
		selfLID:   session.SelfLID,
		peerLID:   session.PeerLID,
		direction: session.Direction,
		codec:     session.Codec,
		answered:  true,
		started:   true,
		cancel:    cancel,
		sendHeld:  held,
	}
	mc := &MediaCall{eng: e, call: call, cancel: cancel, done: make(chan struct{}), held: held}
	runKey := bytes.Clone(session.CallKey)
	inbound := session.Direction == CallDirectionIncoming
	c.log.Info().Str("call_id", session.CallID).Int("relay_endpoints", len(rd.endpoints)).
		Bool("sending_held", held != nil).Msg("starting handed-off media")

	// Source of truth: https://github.com/Hellysonrp/meowcaller/blob/9259460582560c1dcc6da66ec9c94abc15c71b70/datasheets/media-handoff.md#L43
	go func() {
		defer close(mc.done)
		defer clear(runKey)
		err := e.runMedia(mctx, session.CallID, call, runKey, session.SelfLID, session.PeerLID, rd, inbound)
		switch {
		case err == nil:
			c.log.Info().Str("call_id", session.CallID).Msg("handed-off media ended")
		case errors.Is(err, context.Canceled):
			c.log.Info().Str("call_id", session.CallID).Msg("handed-off media stopped")
		default:
			c.log.Warn().Err(err).Str("call_id", session.CallID).Msg("handed-off media failed")
		}
		mc.err = err
		e.finishCall(session.CallID, "media ended")
	}()
	return mc, nil
}

// Play attaches a new Player for src as what the peer hears, and returns it.
func (c *MediaCall) Play(src AudioSource) *Player {
	// Source of truth: https://github.com/Hellysonrp/meowcaller/blob/9259460582560c1dcc6da66ec9c94abc15c71b70/datasheets/media-handoff.md#L41
	return c.call.Play(src)
}

// Subscribe attaches p as what the peer hears.
func (c *MediaCall) Subscribe(p *Player) {
	// Source of truth: https://github.com/Hellysonrp/meowcaller/blob/9259460582560c1dcc6da66ec9c94abc15c71b70/datasheets/media-handoff.md#L41
	c.call.Subscribe(p)
}

// Receive attaches sink for the peer's decoded audio.
func (c *MediaCall) Receive(sink AudioSink) {
	// Source of truth: https://github.com/Hellysonrp/meowcaller/blob/9259460582560c1dcc6da66ec9c94abc15c71b70/datasheets/media-handoff.md#L41
	c.call.Receive(sink)
}

// OnReady registers fn for the call's first inbound audio.
func (c *MediaCall) OnReady(fn func()) {
	// Source of truth: https://github.com/Hellysonrp/meowcaller/blob/9259460582560c1dcc6da66ec9c94abc15c71b70/datasheets/media-handoff.md#L41
	c.call.OnReady(fn)
}

// Rekey points the call's inbound media at peerLID, as the signaling side reports it
// through MediaHandoff.PeerChanged.
func (c *MediaCall) Rekey(peerLID string) error {
	// Source of truth: https://github.com/Hellysonrp/meowcaller/blob/9259460582560c1dcc6da66ec9c94abc15c71b70/datasheets/media-handoff.md#L42
	// NOT VALIDATED: validated once a live call's inbound audio follows a peer change reported after its media started.
	if peerLID == "" {
		return nil
	}
	e := c.eng
	e.mu.Lock()
	m := e.calls[c.call.id]
	if m == nil {
		e.mu.Unlock()
		return errors.New("meowcaller: media call has ended")
	}
	if peerLID == m.peerLID {
		e.mu.Unlock()
		return nil
	}
	m.peerLID = peerLID
	rekeyPeer := m.rekeyPeer
	e.mu.Unlock()
	if rekeyPeer == nil {
		return nil
	}
	return rekeyPeer(peerLID)
}

// StartSending ends a hold set by WithSendingHeld: sending starts with the next frame.
// It does nothing for media run without the hold, and on a repeated call.
func (c *MediaCall) StartSending() {
	// Source of truth: https://github.com/Hellysonrp/meowcaller/blob/3d49711d486ddb54af1c918043f93940061787bb/datasheets/sending-held.md#L21-L22
	if c.held != nil && c.held.CompareAndSwap(true, false) {
		c.eng.c.log.Info().Str("call_id", c.call.id).Msg("handed-off media sending started")
	}
}

// Stop ends the call's media. Done closes once the media loop has exited.
func (c *MediaCall) Stop() {
	// Source of truth: https://github.com/Hellysonrp/meowcaller/blob/9259460582560c1dcc6da66ec9c94abc15c71b70/datasheets/media-handoff.md#L44
	c.cancel()
}

// Done is closed once the call's media has ended.
func (c *MediaCall) Done() <-chan struct{} {
	// Source of truth: https://github.com/Hellysonrp/meowcaller/blob/9259460582560c1dcc6da66ec9c94abc15c71b70/datasheets/media-handoff.md#L43
	return c.done
}

// Err is the media loop's error once Done is closed, and nil before.
func (c *MediaCall) Err() error {
	// Source of truth: https://github.com/Hellysonrp/meowcaller/blob/9259460582560c1dcc6da66ec9c94abc15c71b70/datasheets/media-handoff.md#L43
	select {
	case <-c.done:
		return c.err
	default:
		return nil
	}
}
