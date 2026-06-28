package mqtt

import "errors"

// QoSOrchestrator abstracts the QoS1 and QoS2 state machines, packet identifier
// allocation, retransmission timers and retry logic for both outbound and
// inbound QoS flows.
//
// A single implementation is used for the whole Client.
type QoSOrchestrator interface {
	// PacketIdentifier allocates a new non-zero packet identifier for the given
	// outbound packet type, QoS and payload size. The orchestrator may return
	// an error immediately (before any packet is sent) if it cannot accept the
	// message (exhausted identifiers, resource limits, invalid combination, etc.).
	PacketIdentifier(pt PacketType, qos QoSLevel, payloadSize int) (uint16, error)

	// OnPublishSent records a just-transmitted QoS>0 PUBLISH so retransmission
	// timers can be armed and in-flight state tracked.
	OnPublishSent(h Header, v VariablesPublish, payload []byte) error

	// OnControlPacket is called for every inbound QoS control packet
	// (PUBACK, PUBREC, PUBREL, PUBCOMP, and inbound PUBLISH with QoS>0).
	OnControlPacket(h Header, packetID uint16) error

	// NextRetransmit returns the next retransmission action, if any, that the
	// Client should perform on the wire. The Client is expected to call this
	// from its HandleNext loop.
	NextRetransmit() (RetransmitAction, bool)

	// Reset clears all in-flight state. Intended to be called by user code
	// after a disconnect if a clean slate is desired.
	Reset()
}

// ErrPacketIDExhausted is returned by QoSOrchestrator implementations when
// no more packet identifiers are available.
var ErrPacketIDExhausted = errors.New("natiu-mqtt: packet identifier space exhausted")

// RetransmitAction describes a packet the orchestrator wants retransmitted.
type RetransmitAction struct {
	Header            Header
	Variables         VariablesPublish
	Payload           []byte // for PUBLISH retransmits
	IsControl         bool
	ControlPacketType PacketType // PUBREL etc.
	ControlPacketID   uint16
}
