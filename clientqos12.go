package mqtt

import (
	"errors"
)

// ClientQoS12 wraps a QoS0 Client and adds QoS1/QoS2 support via a
// user-supplied QoSOrchestrator.
type ClientQoS12 struct {
	*Client
	cfg ClientQoS12Config

	// _nanotime is the time source override for ClientQoS12.
	_nanotime func() int64
}

// ClientQoS12Config configures a ClientQoS12 instance.
type ClientQoS12Config struct {
	// Orchestrator handles all QoS1/QoS2 state. Required for QoS>0 operations.
	Orchestrator QoSOrchestrator
	// DefaultQoS is the QoS level used for PublishPayload when the caller does

	// Nanotime returns the current time in nanoseconds.
	// When nil, the underlying Client's time source is used.
	// Useful for deterministic tests that simulate time.
	Nanotime func() int64
}

// NewClientQoS12 creates a ClientQoS12 that re-uses the supplied base Client
// for transport and QoS0 handling.
func NewClientQoS12(base *Client, cfg ClientQoS12Config) *ClientQoS12 {
	return &ClientQoS12{Client: base, cfg: cfg, _nanotime: cfg.Nanotime}
}

// PublishPayload sends a PUBLISH. When QoS > 0 it obtains a packet identifier
// from the orchestrator before transmitting; any error from the orchestrator
// is returned immediately.
func (c *ClientQoS12) PublishPayload(flags PacketFlags, varPub VariablesPublish, payload []byte) error {
	qos := flags.QoS()
	if qos == QoS0 {
		return c.Client.PublishPayload(flags, varPub, payload)
	}
	if c.cfg.Orchestrator == nil {
		return errors.New("QoSOrchestrator required for QoS>0")
	}
	id, err := c.cfg.Orchestrator.PacketIdentifier(PacketPublish, qos, len(payload))
	if err != nil {
		return err
	}
	varPub.PacketIdentifier = id
	// Build header with the requested QoS bits
	h := newHeader(PacketPublish, flags.WithQoS(qos), uint32(varPub.Size(qos)+len(payload)))
	if err := c.cfg.Orchestrator.OnPublishSent(h, varPub, payload); err != nil {
		return err
	}
	return c.Client.txWritePublish(h, varPub, payload)
}

// txWritePublish is a small helper to avoid duplicating the low-level write.
func (c *Client) txWritePublish(h Header, varPub VariablesPublish, payload []byte) error {
	c.txlock.Lock()
	defer c.txlock.Unlock()
	if !c.IsConnected() {
		return errDisconnected
	}
	return c.tx.WritePublishPayload(h, varPub, payload)
}

// WithQoS returns a new PacketFlags with the QoS bits set.
func (pf PacketFlags) WithQoS(qos QoSLevel) PacketFlags {
	return (pf &^ qosbits) | PacketFlags(qos<<1)
}
