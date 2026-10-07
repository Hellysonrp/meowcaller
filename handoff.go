package meowcaller

import (
	"bytes"
	"errors"
)

// errNotImplemented is returned by scaffolded bodies that have not landed yet.
var errNotImplemented = errors.New("meowcaller: not implemented")

// MediaHandoff receives a 1:1 call's media session in place of the engine running it.
type MediaHandoff interface {
	// StartMedia receives the call's media session, once, when the engine would have
	// started its media.
	StartMedia(session MediaSession)
	// PeerChanged reports a later change of the call's peer: the relay-elected peer
	// device or the device that answered.
	PeerChanged(callID, peerLID string)
}

// MediaSession is everything RunMedia needs to run one 1:1 call's media. CallKey and
// the relay key and tokens are secrets.
type MediaSession struct {
	CallID    string
	CallKey   []byte
	SelfLID   string
	PeerLID   string
	Direction CallDirection
	Codec     AudioCodec
	Relay     MediaSessionRelay
}

// MediaSessionRelay is the relay allocation a call's media connects to.
type MediaSessionRelay struct {
	Key       []byte
	Tokens    [][]byte
	Endpoints []MediaSessionEndpoint
}

// MediaSessionEndpoint is one relay endpoint offered for a call.
type MediaSessionEndpoint struct {
	RelayID     uint32
	RelayName   string
	TokenID     uint32
	AuthTokenID uint32
	IsFNA       bool
	Addresses   []MediaSessionAddress
}

// MediaSessionAddress is one IPv4 address of a relay endpoint.
type MediaSessionAddress struct {
	IPv4 string
	Port uint16
}

// WithMediaHandoff makes a Client hand each 1:1 call's media session to h instead of
// running the media itself.
func WithMediaHandoff(h MediaHandoff) Option {
	// Source of truth: https://github.com/Hellysonrp/meowcaller/blob/9259460582560c1dcc6da66ec9c94abc15c71b70/datasheets/media-handoff.md#L31
	return func(c *config) { c.mediaHandoff = h }
}

func newMediaSessionRelay(rd *relayData) MediaSessionRelay {
	// Source of truth: https://github.com/Hellysonrp/meowcaller/blob/9259460582560c1dcc6da66ec9c94abc15c71b70/datasheets/media-handoff.md#L37-L38
	if rd == nil {
		return MediaSessionRelay{}
	}
	out := MediaSessionRelay{
		Key:    bytes.Clone(rd.relayKeyASCII),
		Tokens: cloneByteSlices(rd.relayTokens),
	}
	for _, ep := range rd.endpoints {
		endpoint := MediaSessionEndpoint{
			RelayID:     ep.relayID,
			RelayName:   ep.relayName,
			TokenID:     ep.tokenID,
			AuthTokenID: ep.authTokenID,
			IsFNA:       ep.isFNA,
		}
		for _, addr := range ep.addresses {
			endpoint.Addresses = append(endpoint.Addresses, MediaSessionAddress{IPv4: addr.ipv4, Port: addr.port})
		}
		out.Endpoints = append(out.Endpoints, endpoint)
	}
	return out
}

func (r MediaSessionRelay) relayData() *relayData {
	// Source of truth: https://github.com/Hellysonrp/meowcaller/blob/9259460582560c1dcc6da66ec9c94abc15c71b70/datasheets/media-handoff.md#L37-L38
	rd := &relayData{
		relayKeyASCII: bytes.Clone(r.Key),
		relayTokens:   cloneByteSlices(r.Tokens),
	}
	for _, endpoint := range r.Endpoints {
		ep := relayEndpoint{
			relayID:     endpoint.RelayID,
			relayName:   endpoint.RelayName,
			tokenID:     endpoint.TokenID,
			authTokenID: endpoint.AuthTokenID,
			isFNA:       endpoint.IsFNA,
		}
		for _, addr := range endpoint.Addresses {
			ep.addresses = append(ep.addresses, relayAddress{ipv4: addr.IPv4, port: addr.Port})
		}
		rd.endpoints = append(rd.endpoints, ep)
	}
	return rd
}

func mediaSessionLocked(callID string, m *engineCall) MediaSession {
	// Source of truth: https://github.com/Hellysonrp/meowcaller/blob/9259460582560c1dcc6da66ec9c94abc15c71b70/datasheets/media-handoff.md#L37-L38
	return MediaSession{
		CallID:    callID,
		CallKey:   bytes.Clone(m.callKey),
		SelfLID:   m.selfLID,
		PeerLID:   m.peerLID,
		Direction: m.direction,
		Codec:     m.codec,
		Relay:     newMediaSessionRelay(m.relay),
	}
}

// prepareHandoffLocked marks callID's media started and prepares its handoff to h. The
// caller holds e.mu and runs the returned func after releasing it.
func (e *engine) prepareHandoffLocked(callID string, m *engineCall, h MediaHandoff) func() {
	// Source of truth: https://github.com/Hellysonrp/meowcaller/blob/9259460582560c1dcc6da66ec9c94abc15c71b70/datasheets/media-handoff.md#L31-L36
	m.started = true
	if m.group {
		return func() {
			e.c.log.Debug().Str("call_id", callID).Msg("media handoff skips a group call")
		}
	}
	session := mediaSessionLocked(callID, m)
	call := m.call
	inbound := m.direction == CallDirectionIncoming
	forward := func(peerLID string) error {
		h.PeerChanged(callID, peerLID)
		return nil
	}
	return func() {
		if call != nil && !(inbound && call.State() == CallPhaseRinging) {
			call.setPhase(CallPhaseConnecting)
		}
		e.c.log.Info().Str("call_id", callID).Int("relay_endpoints", len(session.Relay.Endpoints)).Msg("handing media off")
		h.StartMedia(session)

		// The hook goes in only after StartMedia returns, as runMedia does after its
		// relay connects, so no PeerChanged precedes the session; a peer change in
		// between is read back from m.peerLID.
		e.mu.Lock()
		current := session.PeerLID
		if m := e.calls[callID]; m != nil {
			m.rekeyPeer = forward
			current = m.peerLID
		}
		e.mu.Unlock()
		if current != "" && current != session.PeerLID {
			h.PeerChanged(callID, current)
		}
	}
}
