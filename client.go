package mqtt

import (
	"context"
	"errors"
)

// Config is a generic
type Config struct {
	Decoder Decoder
	// OnError is called on errors during MQTT operation. If OnError returns
	// an error itself then Client should shutdown gracefully and give user error.
	OnError func(error) error
	Ctx     context.Context
	err     error
}

// SetError sets an error during configuration such that
// NewClient fails and returns that error.
func (cfg *Config) SetError(err error) {
	if cfg.err == nil {
		cfg.err = err // First error encountered wins.
	}
}

type ConfigOption func(*Config)

func WithClientConfig(cfg Config) ConfigOption {
	return func(c *Config) {
		*c = cfg
	}
}

func DefaultClientConfig() ConfigOption {
	return func(c *Config) {
		if c.Decoder == nil {
			c.Decoder = DecoderLowmem{UserBuffer: make([]byte, 1500)}
		}
		if c.Ctx == nil {
			c.Ctx = context.Background()
		}
		if c.OnError == nil {
			c.OnError = func(err error) error { return err }
		}
	}
}

func applyConfigs(cfg *Config, cfgs []ConfigOption) error {
	if len(cfgs) == 0 {
		cfgs = []ConfigOption{DefaultClientConfig()}
	}
	for _, option := range cfgs {
		option(cfg)
		if cfg.err != nil {
			return cfg.err
		}
	}
	if cfg.Decoder == nil {
		return errors.New("nil Decoder or not set")
	}
	return nil
}
