package mqtt

import "errors"

// packetType takes on values 1..14. It is the first 4 bits of a control packet.
type packetType byte

const (
	_ packetType = iota // 0 Forbidden/Reserved
	// Client to Server - Client request to connect to Server
	ptConnect
	// Server to Client - Connect acknowledgment
	ptConnack
	ptPublish
	ptPuback
	// Publish received. assured delivery part 1
	ptPubrec
	// Publish release. Assured delivery part 2.
	ptPubrel
	// Publish complete. Assured delivery part 3.
	ptPubcomp
	// Subscribe - client subscribe request.
	ptSubscribe
	ptSuback
	ptUnsubscribe
	ptUnsuback
	ptPingreq
	ptPingresp
	ptDisconnect
)

func (p packetType) marshal(flagbits byte) byte {
	if p > 15 || flagbits > 15 {
		panic("arguments out of 0..15 4 bit range")
	}
	return byte(p)<<4 | flagbits
}

func (p packetType) ValidateFlags(flag4bits byte) error {
	onlyBit1Set := flag4bits&^(1<<1) == 0
	isControlPacket := p == ptPubrel || p == ptSubscribe || p == ptUnsubscribe
	if p == ptPublish || (onlyBit1Set && isControlPacket) || (!isControlPacket && flag4bits == 0) {
		return nil
	}
	if isControlPacket {
		return errors.New("expected flag 0b0010")
	}
	return errors.New("expected flag 0")
}

func (p packetType) String() string {
	if p > 15 {
		return "impossible packet type value" // Exceeds 4 bit value.
	}
	var s string
	switch p {
	case ptConnect:
		s = "CONNECT"
	case ptConnack:
		s = "CONNACK"
	case ptPuback:
		s = "PUBACK"
	case ptPubcomp:
		s = "PUBCOMP"
	case ptPublish:
		s = "PUBLISH"
	case ptPubrec:
		s = "PUBREC"
	case ptPubrel:
		s = "PUBREL"
	case ptSubscribe:
		s = "SUBSCRIBE"
	case ptUnsubscribe:
		s = "UNSUBSCRIBE"
	case ptUnsuback:
		s = "UNSUBACK"
	case ptSuback:
		s = "SUBACK"
	case ptPingresp:
		s = "PINGRESP"
	case ptPingreq:
		s = "PINGREQ"
	case ptDisconnect:
		s = "DISCONNECT"
	default:
		s = "forbidden"
	}
	return s
}

type controlPacket struct {
	PacketType packetType // The type of the MQTT control packet
	Flags      byte       // Flags for the control packet
	Payload    []byte     // The payload of the control packet
}

// decodeRemainingLength expects
func decodeRemainingLength(b []byte) (value uint32, err error) {
	multiplier := uint32(1)
	for i := 0; i < maxRemainingLengthSize; i++ {
		encodedByte := b[i]
		value += uint32(encodedByte&127) * multiplier
		if encodedByte&128 != 0 {
			return value, nil
		}
		multiplier *= 128
	}
	return 0, errors.New("malformed remaining length")
}

func encodeRemainingLength(remlen uint32, b []byte) (n int) {
	if remlen > maxRemainingLengthSize {
		panic("remaining length too large")
	}
	for n = 0; remlen > 0 && n < maxRemainingLengthSize; n++ {
		encoded := byte(remlen % 128)
		remlen /= 128
		if remlen > 0 {
			encoded |= 128
		}
		b[n] = encoded
	}
	return n
}

// Publish packets only contain PI if QoS > 0
func (p packetType) containsPacketIdentifier() bool {
	if p == ptPublish {
		panic("unsupported on publish, PI depends on QoS")
	}
	noPI := p == ptConnect || p == ptConnack ||
		p == ptPingreq || p == ptPingresp || p == ptDisconnect
	return !noPI
}

func (p packetType) containsPayload() bool {
	if p == ptPublish {
		panic("call unsupported on publish (payload optional)")
	}
	return p == ptConnect || p == ptSubscribe || p == ptSuback ||
		p == ptUnsubscribe || p == ptUnsuback
}
