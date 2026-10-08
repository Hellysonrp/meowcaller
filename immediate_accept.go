package meowcaller

// WithImmediateAccept makes a Client answer a 1:1 incoming call by sending its accept
// from Answer, instead of on the caller's first mute_v2. It changes only when the accept
// goes out: media the Client runs itself still starts at the offer.
func WithImmediateAccept() Option {
	// Source of truth: https://github.com/Hellysonrp/meowcaller/blob/a841a18ea50ccbd1a14212ca5d481dc22c54c007/datasheets/immediate-accept.md#L21-L24
	return func(c *config) { c.immediateAccept = true }
}
