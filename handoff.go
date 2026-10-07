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
	// TODO
	// agent suggestion: return an Option that stores h in config.mediaHandoff; NewClient copies it onto the Client.
	// human input:
	return func(*config) {}
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
	// TODO
	// agent suggestion: set m.started; for a group call return a func that only logs at debug; otherwise build the session, point m.rekeyPeer at h.PeerChanged, and return a func that applies the Connecting rule (a ringing inbound call stays Ringing), logs the endpoint count and calls h.StartMedia.
	// human input:
	return func() {}
}
