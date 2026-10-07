package meowcaller

import "errors"

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
	// TODO
	// agent suggestion: copy the relay key, the indexed tokens (nil gaps kept) and every endpoint and address field for field, cloning each byte slice; leave peerJID out.
	// human input:
	return MediaSessionRelay{}
}

func (r MediaSessionRelay) relayData() *relayData {
	// Source of truth: https://github.com/Hellysonrp/meowcaller/blob/9259460582560c1dcc6da66ec9c94abc15c71b70/datasheets/media-handoff.md#L37-L38
	// TODO
	// agent suggestion: the inverse of newMediaSessionRelay, cloning each byte slice; peerJID stays zero.
	// human input:
	return &relayData{}
}

func mediaSessionLocked(callID string, m *engineCall) MediaSession {
	// Source of truth: https://github.com/Hellysonrp/meowcaller/blob/9259460582560c1dcc6da66ec9c94abc15c71b70/datasheets/media-handoff.md#L37-L38
	// TODO
	// agent suggestion: callID, a clone of m.callKey, m.selfLID, m.peerLID, m.direction, m.codec and newMediaSessionRelay(m.relay).
	// human input:
	return MediaSession{}
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
