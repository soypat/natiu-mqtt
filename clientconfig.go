package mqtt

type ClientConfig struct {
	ReadBuffer  []byte
	WriteBuffer []byte
	err         error
}

// SetError sets an error during configuration such that
// NewClient fails and returns that error.
func (cfg *ClientConfig) SetError(err error) {
	cfg.err = err
}

type ClientOption func(*ClientConfig)

func WithClientConfig(cfg ClientConfig) ClientOption {
	return func(c *ClientConfig) {
		*c = cfg
	}
}

func DefaultClientConfig() ClientOption {
	return func(c *ClientConfig) {
		if len(c.ReadBuffer) == 0 {
			c.ReadBuffer = make([]byte, defaultBufferLen)
		}
		if len(c.WriteBuffer) == 0 {
			c.WriteBuffer = make([]byte, defaultBufferLen)
		}
	}
}
