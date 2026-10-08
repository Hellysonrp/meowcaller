package meowcaller

// WithImmediateAccept makes a Client answer a 1:1 incoming call by sending its accept
// from Answer, instead of on the caller's first mute_v2.
func WithImmediateAccept() Option {
	// Source of truth: https://github.com/Hellysonrp/meowcaller/blob/a841a18ea50ccbd1a14212ca5d481dc22c54c007/datasheets/immediate-accept.md#L21-L24
	// TODO
	// agent suggestion: return an Option that sets config.immediateAccept; NewClient copies it onto its Client, and answer sends a 1:1 accept at once when it is set.
	// human input:
	return func(*config) {}
}
