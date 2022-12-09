package mqtt

import (
	"errors"
	"time"
)

type Client struct {
	nextPacketID    uint
	cmdTimeout      time.Duration
	keepalive       uint16
	pingOutstanding byte
	isConnected     int
	cleanSession    bool
	decoder         Decoder
	msgHandlers     [maxMessageHandlers]struct {
		topicFilter string
		handler     func(*Header)
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
	if cfg.Decoder == nil {
		return Client{}, errors.New("nil Decoder or not set")
	}
	return Client{
		nextPacketID: 1,
		decoder:      cfg.Decoder,
	}, nil
}

type ClientConfig struct {
	Decoder Decoder
	err     error
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
		if c.Decoder == nil {
			c.Decoder = DecoderLowmem{UserBuffer: make([]byte, 1500)}
		}
	}
}
