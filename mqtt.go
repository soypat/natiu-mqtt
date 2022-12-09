package mqtt

import (
	"errors"
)

const bugReportLink = "Please report bugs at https://github.com/soypat/natiu-mqtt/issues/new "

var (
	errQoS0NoDup = errors.New("DUP must be 0 for all QoS0 [MQTT-3.3.1-2]")
	errGotZeroPI = errors.New("packet identifier must be nonzero for packet type")
)

type transportfunc = func() (byte, error)

// Header represents the bytes preceding the payload in an MQTT packet.
// This commonly called the Fixed Header, although this Header type also contains
// PacketIdentifier, which is part of the Variable Header and may or may not be present
// in an MQTT packet.
type Header struct {
	firstByte        byte
	RemainingLength  uint32
	PacketIdentifier uint16
}

// PacketFlags represents the LSB 4 bits in the first byte in an MQTT fixed header.
// PacketFlags takes on select values in range 1..15. PacketType and PacketFlags are present in all MQTT packets.
type PacketFlags uint8

func (pf PacketFlags) QoS() QoSLevel   { return QoSLevel((pf >> 1) & 0b11) }
func (pf PacketFlags) Retain() bool    { return pf&1 != 0 }
func (pf PacketFlags) Duplicate() bool { return pf&(1<<3) != 0 }
func (pf PacketFlags) String() string {
	if pf > 15 {
		return "invalid packet flags"
	}
	s := pf.QoS().String()
	if pf.Duplicate() {
		s += "/DUP"
	}
	if pf.Retain() {
		s += "/RET"
	}
	return s
}

// NewPublishFlags returns PUBLISH packet flags and an error if the flags were
// to create a malformed packet according to MQTT specification.
func NewPublishFlags(qos QoSLevel, dup, retain bool) (PacketFlags, error) {
	if qos > QoS2 {
		return 0, errors.New("invalid QoS")
	}
	if dup && qos == QoS0 {
		return 0, errQoS0NoDup
	}
	return PacketFlags(b2u8(retain) | (b2u8(dup) << 3) | uint8(qos<<1)), nil
}

// NewHeader creates a new Header for a packetType and returns an error if invalid
// arguments are passed in. It will set expected reserved flags for non-PUBLISH packets.
func NewHeader(packetType PacketType, packetFlags PacketFlags, identifier uint16, remainingLen uint32) (Header, error) {
	if packetType != PacketPublish {
		// Set reserved flag for non-publish packets.
		ctlBit := b2u8(packetType == PacketPubrel || packetType == PacketSubscribe || packetType == PacketUnsubscribe)
		packetFlags = PacketFlags(ctlBit << 1)
	}
	if packetFlags > 15 {
		return Header{}, errors.New("packet flags exceeds 4 bit range 0..15")
	}
	if packetType > 15 {
		return Header{}, errors.New("packet type exceeds 4 bit range 0..15")
	}
	h := newHeader(packetType, packetFlags, identifier, remainingLen)
	if err := h.Validate(); err != nil {
		return Header{}, err
	}
	return h, nil
}

func newHeader(pt PacketType, pf PacketFlags, identifier uint16, rlen uint32) Header {
	return Header{ // Creates a header with no error checking. For internal use.
		firstByte:        byte(pt)<<4 | byte(pf),
		RemainingLength:  rlen,
		PacketIdentifier: identifier,
	}
}

// Validate returns an error if the Header contains malformed data. This usually means
// the header has bits set that contradict "MUST" statements in MQTT's protocol specification.
func (h Header) Validate() error {
	pflags := h.Flags()
	ptype := h.Type()
	err := ptype.ValidateFlags(pflags)
	if err != nil {
		return err
	}
	hasPI := ptype.containsPacketIdentifier(pflags)
	if hasPI && h.PacketIdentifier == 0 {
		return errGotZeroPI
	}
	if ptype == PacketPublish {
		dup := pflags.Duplicate()
		qos := pflags.QoS()
		if qos > QoS2 {
			return errors.New("invalid QoS")
		}
		if dup && qos == QoS0 {
			return errQoS0NoDup
		}
	}
	return nil
}

func (h Header) Flags() PacketFlags { return PacketFlags(h.firstByte & 0b1111) }
func (h Header) Type() PacketType   { return PacketType(h.firstByte >> 4) }
func (h Header) String() string {
	return h.Type().String() + " " + h.Flags().String()
}

// PacketType represents the 4 MSB bits in the first byte in an MQTT fixed header.
// takes on values 1..14. PacketType and PacketFlags are present in all MQTT packets.
type PacketType byte

const (
	_ PacketType = iota // 0 Forbidden/Reserved
	// Client to Server - Client request to connect to a Server.
	// After a network connection is established by a client to a server, the first
	// packet sent from the client to the server must be a Connect packet.
	PacketConnect
	// Server to Client - Connect acknowledgment
	PacketConnack
	PacketPublish
	PacketPuback
	// Publish received. assured delivery part 1
	PacketPubrec
	// Publish release. Assured delivery part 2.
	PacketPubrel
	// Publish complete. Assured delivery part 3.
	PacketPubcomp
	// Subscribe - client subscribe request.
	PacketSubscribe
	PacketSuback
	PacketUnsubscribe
	PacketUnsuback
	PacketPingreq
	PacketPingresp
	PacketDisconnect
)

func (p PacketType) ValidateFlags(flag4bits PacketFlags) error {
	onlyBit1Set := flag4bits&^(1<<1) == 0
	isControlPacket := p == PacketPubrel || p == PacketSubscribe || p == PacketUnsubscribe
	if p == PacketPublish || (onlyBit1Set && isControlPacket) || (!isControlPacket && flag4bits == 0) {
		return nil
	}
	if isControlPacket {
		return errors.New("control packet bit not set (0b0010)")
	}
	return errors.New("expected 0b0000 flag for packet type")
}

func (p PacketType) String() string {
	if p > 15 {
		return "impossible packet type value" // Exceeds 4 bit value.
	}
	var s string
	switch p {
	case PacketConnect:
		s = "CONNECT"
	case PacketConnack:
		s = "CONNACK"
	case PacketPuback:
		s = "PUBACK"
	case PacketPubcomp:
		s = "PUBCOMP"
	case PacketPublish:
		s = "PUBLISH"
	case PacketPubrec:
		s = "PUBREC"
	case PacketPubrel:
		s = "PUBREL"
	case PacketSubscribe:
		s = "SUBSCRIBE"
	case PacketUnsubscribe:
		s = "UNSUBSCRIBE"
	case PacketUnsuback:
		s = "UNSUBACK"
	case PacketSuback:
		s = "SUBACK"
	case PacketPingresp:
		s = "PINGRESP"
	case PacketPingreq:
		s = "PINGREQ"
	case PacketDisconnect:
		s = "DISCONNECT"
	default:
		s = "forbidden/reserved packet type"
	}
	return s
}

// Publish packets only contain PI if QoS > 0
func (p PacketType) containsPacketIdentifier(flags PacketFlags) bool {
	if p == PacketPublish {
		return flags.QoS() > 0
	}
	noPI := p == PacketConnect || p == PacketConnack ||
		p == PacketPingreq || p == PacketPingresp || p == PacketDisconnect
	return p != 0 && p < 15 && !noPI // Robust condition, returns true only for valid packets.
}

func (p PacketType) containsPayload() bool {
	if p == PacketPublish {
		panic("call unsupported on publish (payload optional)")
	}
	return p == PacketConnect || p == PacketSubscribe || p == PacketSuback ||
		p == PacketUnsubscribe || p == PacketUnsuback
}

// QoSLevel represents the Quality of Service specified by the client.
// The server can choose to provide or reject requested QoS. The values
// of QoS range from 0 to 2, each representing a differnt methodology for
// message delivery guarantees.
type QoSLevel uint8

// QoS indicates the level of assurance for packet delivery.
const (
	// QoS0 at most once delivery. Arrives either once or not at all. Depends on capabilities of underlying network.
	QoS0 QoSLevel = iota
	// QoS1 at least once delivery. Ensures message arrives at receiver at least once.
	QoS1
	// QoS2 Exactly once delivery. Highest quality service. For use when neither loss nor duplication of messages are acceptable.
	// There is an increased overhead associated with this quality of service.
	QoS2
	// Reserved, must not be used.
	reservedQoS3
	// QoSSubfail marks a failure in SUBACK. This value cannot be encoded into a header.
	QoSSubfail QoSLevel = 0x80
)

// IsValid returns true if qos is a valid Quality of Service or if it is
// the SUBACK response failure to subscribe return code.
func (qos QoSLevel) IsValid() bool { return qos <= QoS2 || qos == QoSSubfail }

func (qos QoSLevel) String() (s string) {
	switch qos {
	case QoS0:
		s = "QoS0"
	case QoS1:
		s = "QoS1"
	case QoS2:
		s = "QoS2"
	case QoSSubfail:
		s = "QoS subscribe failure"
	case reservedQoS3:
		s = "invalid: use of reserved QoS3"
	default:
		s = "undefined QoS"
	}
	return s
}

// Packet specific functions

// VariablesConnect all strings in the variable header must be UTF-8 encoded
// except password which may be binary data.
type VariablesConnect struct {
	// Must be present and unique to the server. UTF-8 encoded string
	// between 1 and 23 bytes in length although some servers may allow larger ClientIDs.
	ClientID     string
	Username     string
	Password     string
	WillTopic    string
	WillMessage  string
	WillRetain   bool
	CleanSession bool
	WillQoS      QoSLevel
	KeepAlive    uint16
}

// Flags returns the eighth CONNECT packet byte.
func (cv *VariablesConnect) Flags() byte {
	willFlag := cv.WillFlag()
	hasUsername := cv.Username != ""
	return b2u8(hasUsername)<<7 | b2u8(hasUsername && cv.Password != "")<<6 | // See  [MQTT-3.1.2-22].
		b2u8(cv.WillRetain)<<5 | byte(cv.WillQoS&0b11)<<3 |
		b2u8(willFlag)<<2 | b2u8(cv.CleanSession)<<1
}

func (cv *VariablesConnect) WillFlag() bool { return cv.WillTopic != "" && cv.WillMessage != "" }

// VarConnack TODO

// VariablesPublish
type VariablesPublish struct {
	// Must be present as utf-8 encoded string with NO wildcard characters.
	// The server may override the TopicName on response according to matching process [Section 4.7]
	TopicName string
	// Only present (non-zero) in QoS level 1 or 2.
	PacketIdentifier uint16
}

type VariablesSubscribe struct {
	PacketIdentifier uint16
	TopicFilters     []SubscribeRequest
}

type SubscribeRequest struct {
	// utf8 encoded topic or match pattern for topic filter.
	Topic string
	// The desired QoS level.
	QoS QoSLevel
}

type VariablesSuback struct {
	PacketIdentifier uint16
	// Each return code corresponds to a topic filter in the SUBSCRIBE
	// packet being acknowledged. These MUST match the order of said SUBSCRIBE packet.
	// A return code can indicate failure using QoSSubfail.
	ReturnCodes []QoSLevel
}

type VariablesUnsubscribe struct {
	PacketIdentifier uint16
	Topics           []string
}

type VariablesConnack struct {
	// Octet with SP (Session Present) on LSB bit0.
	AckFlags uint8
	// Octet
	ReturnCode ConnectReturnCode
}

// validate provides early validation of CONNACK variables.
func (vc VariablesConnack) validate() error {
	if vc.AckFlags&^1 != 0 {
		return errors.New("CONNACK Ack flag bits 7-1 must be set to 0")
	}
	return nil
}

type ConnectReturnCode uint8

const (
	ReturnCodeConnAccepted ConnectReturnCode = iota
	ReturnCodeUnnaceptableProtocol
	ReturnCodeIdentifierRejected
	ReturnCodeServerUnavailable
	ReturnCodeBadUserCredentials
	ReturnCodeUnauthorized
	minInvalidReturnCode
)

func (rc ConnectReturnCode) String() (s string) {
	switch rc {
	default:
		s = "unknown CONNACK return code"
	case ReturnCodeConnAccepted:
		s = "connection accepted"
	case ReturnCodeUnnaceptableProtocol:
		s = "unacceptable protocol version"
	case ReturnCodeIdentifierRejected:
		s = "client identifier rejected"
	case ReturnCodeBadUserCredentials:
		s = "bad username and/or password"
	case ReturnCodeUnauthorized:
		s = "client unauthorized"
	}
	return s
}
