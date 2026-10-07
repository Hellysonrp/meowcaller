package meowcaller

import "context"

// MediaCall is a 1:1 call whose media runs here while its signaling runs on another
// Client, which handed its session over through MediaHandoff.
type MediaCall struct {
	eng    *engine
	call   *Call
	cancel context.CancelFunc
	done   chan struct{}
	err    error
}

// RunMedia starts a handed-off session's media with no WhatsApp client: the relay
// connection, the codec and the end-to-end SRTP. Done reports when it ends.
func RunMedia(ctx context.Context, session MediaSession, opts ...Option) (*MediaCall, error) {
	// Source of truth: https://github.com/Hellysonrp/meowcaller/blob/9259460582560c1dcc6da66ec9c94abc15c71b70/datasheets/media-handoff.md#L39-L40
	// TODO
	// agent suggestion: validate the call ID, call key, peer LID and relay endpoints; build an engine from a Client with only the logger and recorder; register one engineCall from the session (answered, started, Connecting); run runMedia in a goroutine that records its error, finishes the call and closes Done.
	// human input:
	return nil, errNotImplemented
}

// Play attaches a new Player for src as what the peer hears, and returns it.
func (c *MediaCall) Play(src AudioSource) *Player {
	// Source of truth: https://github.com/Hellysonrp/meowcaller/blob/9259460582560c1dcc6da66ec9c94abc15c71b70/datasheets/media-handoff.md#L41
	// TODO
	// agent suggestion: delegate to the call's Play.
	// human input:
	return nil
}

// Subscribe attaches p as what the peer hears.
func (c *MediaCall) Subscribe(p *Player) {
	// Source of truth: https://github.com/Hellysonrp/meowcaller/blob/9259460582560c1dcc6da66ec9c94abc15c71b70/datasheets/media-handoff.md#L41
	// TODO
	// agent suggestion: delegate to the call's Subscribe.
	// human input:
}

// Receive attaches sink for the peer's decoded audio.
func (c *MediaCall) Receive(sink AudioSink) {
	// Source of truth: https://github.com/Hellysonrp/meowcaller/blob/9259460582560c1dcc6da66ec9c94abc15c71b70/datasheets/media-handoff.md#L41
	// TODO
	// agent suggestion: delegate to the call's Receive.
	// human input:
}

// OnReady registers fn for the call's first inbound audio.
func (c *MediaCall) OnReady(fn func()) {
	// Source of truth: https://github.com/Hellysonrp/meowcaller/blob/9259460582560c1dcc6da66ec9c94abc15c71b70/datasheets/media-handoff.md#L41
	// TODO
	// agent suggestion: delegate to the call's OnReady.
	// human input:
}

// Rekey points the call's inbound media at peerLID, as the signaling side reports it
// through MediaHandoff.PeerChanged.
func (c *MediaCall) Rekey(peerLID string) error {
	// Source of truth: https://github.com/Hellysonrp/meowcaller/blob/9259460582560c1dcc6da66ec9c94abc15c71b70/datasheets/media-handoff.md#L42
	// TODO
	// agent suggestion: ignore an empty or unchanged peer; under e.mu fail if the call has ended, else store peerLID and take rekeyPeer; call it after unlocking when the loop has installed it.
	// human input:
	return errNotImplemented
}

// Stop ends the call's media. Done closes once the media loop has exited.
func (c *MediaCall) Stop() {
	// Source of truth: https://github.com/Hellysonrp/meowcaller/blob/9259460582560c1dcc6da66ec9c94abc15c71b70/datasheets/media-handoff.md#L44
	// TODO
	// agent suggestion: cancel the media loop's context.
	// human input:
}

// Done is closed once the call's media has ended.
func (c *MediaCall) Done() <-chan struct{} {
	// Source of truth: https://github.com/Hellysonrp/meowcaller/blob/9259460582560c1dcc6da66ec9c94abc15c71b70/datasheets/media-handoff.md#L43
	// TODO
	// agent suggestion: return the done channel.
	// human input:
	return nil
}

// Err is the media loop's error once Done is closed, and nil before.
func (c *MediaCall) Err() error {
	// Source of truth: https://github.com/Hellysonrp/meowcaller/blob/9259460582560c1dcc6da66ec9c94abc15c71b70/datasheets/media-handoff.md#L43
	// TODO
	// agent suggestion: the recorded error if done is closed, nil otherwise.
	// human input:
	return errNotImplemented
}
