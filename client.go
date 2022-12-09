package mqtt

import (
	"time"
)

type Client struct {
	nextPacketID    uint
	cmdTimeout      time.Duration
	buf             []byte
	readbuf         []byte
	keepalive       uint16
	pingOutstanding byte
	isConnected     int
	cleanSession    bool
	msgHandlers     [maxMessageHandlers]struct {
		topicFilter string
		handler     func(*Message)
	}
}

func NewClient(configuration ...ClientOption) (Client, error) {
	var cfg ClientConfig
	if len(configuration) == 0 {
		configuration = []ClientOption{DefaultClientConfig()}
	}
	for _, option := range configuration {
		option(&cfg)
		if cfg.err != nil {
			return Client{}, cfg.err
		}
	}
	return Client{
		nextPacketID: 1,
		readbuf:      cfg.ReadBuffer,
		buf:          cfg.WriteBuffer,
	}, nil
}

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
