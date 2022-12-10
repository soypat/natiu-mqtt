package mqtt

import (
	"errors"
	"io"
)

// Decoder provides an abstraction for an MQTT variable header decoding implementation.
// This is because heap allocations are necessary to be able to decode any MQTT packet.
// Some compile targets are restrictive in terms of memory usage, so the best decoder for the situation may differ.
type Decoder interface {
	// TODO(soypat): The CONNACK and SUBACK decoders can probably be excluded
	// from this interface since they do not need heap allocations, or if they
	// do end uf allocating their allocations are short lived, within scope of function.

	// DecodeConnack(r io.Reader) (VariablesConnack, int, error)

	// DecodeSuback(r io.Reader, remainingLen uint32) (VariablesSuback, int, error)

	DecodePublish(r io.Reader, qos QoSLevel) (VariablesPublish, int, error)
	DecodeConnect(r io.Reader) (VariablesConnect, int, error)
	DecodeSubscribe(r io.Reader, remainingLen uint32) (VariablesSubscribe, int, error)
	DecodeUnsubscribe(r io.Reader, remainingLength uint32) (VariablesUnsubscribe, int, error)
}

const bugReportLink = "Please report bugs at https://github.com/soypat/natiu-mqtt/issues/new "

var (
	errQoS0NoDup      = errors.New("DUP must be 0 for all QoS0 [MQTT-3.3.1-2]")
	errGotZeroPI      = errors.New("packet identifier must be nonzero for packet type")
	ErrUserBufferFull = errors.New("natiu-mqtt: user buffer full")
)

// Header represents the bytes preceding the payload in an MQTT packet.
// This commonly called the Fixed Header, although this Header type also contains
// PacketIdentifier, which is part of the Variable Header and may or may not be present
// in an MQTT packet.
type Header struct {
	// firstByte contains packet type in MSB bits 7-4 and flags in LSB bits 3-0.
	firstByte       byte
	RemainingLength uint32
}

// HasPacketIdentifier returns true if the MQTT packet has a 2 octet packet identifier number.
func (hd Header) HasPacketIdentifier() bool {
	tp := hd.Type()
	qos := hd.Flags().QoS()
	if tp == PacketPublish && (qos == 1 || qos == 2) {
		return true
	}
	noPI := tp == PacketConnect || tp == PacketConnack ||
		tp == PacketPingreq || tp == PacketPingresp || tp == PacketDisconnect || tp == PacketPublish
	return tp != 0 && tp < 15 && !noPI
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
func NewHeader(packetType PacketType, packetFlags PacketFlags, remainingLen uint32) (Header, error) {
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
	h := newHeader(packetType, packetFlags, remainingLen)
	if err := h.Validate(); err != nil {
		return Header{}, err
	}
	return h, nil
}

func newHeader(pt PacketType, pf PacketFlags, rlen uint32) Header {
	return Header{ // Creates a header with no error checking. For internal use.
		firstByte:       byte(pt)<<4 | byte(pf),
		RemainingLength: rlen,
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

// PacketType lists in definitions.go

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

// QoSLevel defined in definitions.go

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
	ClientID []byte
	// By default will be set to 'MQTT' protocol if nil, which is v3.1 compliant.
	Protocol []byte
	// By default if set to 0 will use Protocol level 4, which is v3.1 compliant
	ProtocolLevel byte
	Username      []byte
	// For password to be used username must also be set. See [MQTT-3.1.2-22].
	Password    []byte
	WillTopic   []byte
	WillMessage []byte
	// This bit specifies if the Will Message is to be Retained when it is published.
	WillRetain   bool
	CleanSession bool
	// These two bits specify the QoS level to be used when publishing the Will Message.
	WillQoS QoSLevel
	// KeepAlive is a interval measured in seconds. it is the maximum time interval that is
	// permitted to elapse between the point at which the Client finishes transmitting one
	// Control Packet and the point it starts sending the next.
	KeepAlive uint16
}

// Size returns size-on-wire of the CONNECT variable header generated by vs.
func (vs *VariablesConnect) Size() (n int) {
	n += mqttStringSize(vs.Username)
	if len(vs.Username) != 0 {
		n += mqttStringSize(vs.Password) // Make sure password is only added when username is enabled.
	}
	if vs.WillFlag() {
		n += len(vs.WillTopic) + len(vs.WillMessage) + 2
	}
	n += len(vs.ClientID) + len(vs.Protocol) + 4
	return n + 1 + 2 // Add Connect flags and keepalive.
}

// StringsLen returns length of all strings in variable header before being encoded.
// StringsLen is useful to know how much of the user's buffer was consumed during decoding.
func (vs *VariablesConnect) StringsLen() (n int) {
	if len(vs.Username) != 0 {
		n += len(vs.Password) // Make sure password is only added when username is enabled.
	}
	if vs.WillFlag() {
		n += len(vs.WillTopic) + len(vs.WillMessage)
	}
	return len(vs.ClientID) + len(vs.Protocol) + len(vs.Username)
}

// Flags returns the eighth CONNECT packet byte.
func (cv *VariablesConnect) Flags() byte {
	willFlag := cv.WillFlag()
	hasUsername := len(cv.Username) == 0
	return b2u8(hasUsername)<<7 | b2u8(hasUsername && len(cv.Password) == 0)<<6 | // See  [MQTT-3.1.2-22].
		b2u8(cv.WillRetain)<<5 | byte(cv.WillQoS&0b11)<<3 |
		b2u8(willFlag)<<2 | b2u8(cv.CleanSession)<<1
}

func (cv *VariablesConnect) WillFlag() bool {
	return len(cv.WillTopic) != 0 && len(cv.WillMessage) != 0
}

// VarConnack TODO

// VariablesPublish represents the variable header of a PUBLISH packet. It does not
// include the payload with the topic data.
type VariablesPublish struct {
	// Must be present as utf-8 encoded string with NO wildcard characters.
	// The server may override the TopicName on response according to matching process [Section 4.7]
	TopicName []byte
	// Only present (non-zero) in QoS level 1 or 2.
	PacketIdentifier uint16
}

// StringsLen returns length of all strings in variable header before being encoded.
// StringsLen is useful to know how much of the user's buffer was consumed during decoding.
func (vp VariablesPublish) StringsLen() int { return len(vp.TopicName) }

// VariablesSubscribe represents the variable header of a SUBSCRIBE packet.
// It encodes the topic filters requested by a Client and the desired QoS for each topic.
type VariablesSubscribe struct {
	PacketIdentifier uint16
	TopicFilters     []SubscribeRequest
}

// StringsLen returns length of all strings in variable header before being encoded.
// StringsLen is useful to know how much of the user's buffer was consumed during decoding.
func (vs VariablesSubscribe) StringsLen() (n int) {
	for _, sub := range vs.TopicFilters {
		n += len(sub.TopicFilter)
	}
	return n
}

// SubscribeRequest is relevant only to SUBSCRIBE packets where several SubscribeRequest
// each encode a topic filter that is to be matched on the server side and a desired
// QoS for each matched topic.
type SubscribeRequest struct {
	// utf8 encoded topic or match pattern for topic filter.
	TopicFilter []byte
	// The desired QoS level.
	QoS QoSLevel
}

// VariablesSuback represents the variable header of a SUBACK packet.
type VariablesSuback struct {
	PacketIdentifier uint16
	// Each return code corresponds to a topic filter in the SUBSCRIBE
	// packet being acknowledged. These MUST match the order of said SUBSCRIBE packet.
	// A return code can indicate failure using QoSSubfail.
	ReturnCodes []QoSLevel
}

// VariablesUnsubscribe represents the variable header of a UNSUBSCRIBE packet.
type VariablesUnsubscribe struct {
	PacketIdentifier uint16
	Topics           [][]byte
}

// StringsLen returns length of all strings in variable header before being encoded.
// StringsLen is useful to know how much of the user's buffer was consumed during decoding.
func (vs VariablesUnsubscribe) StringsLen() (n int) {
	for _, sub := range vs.Topics {
		n += len(sub)
	}
	return n
}

type VariablesConnack struct {
	// Octet with SP (Session Present) on LSB bit0.
	AckFlags uint8
	// Octet
	ReturnCode ConnectReturnCode
}

// SessionPresent returns true if the SP bit is set in the CONNACK Ack flags. This bit indicates whether
// the ClientID already has a session on the server.
//   - If server accepts a connection with CleanSession set to 1 the server MUST set SP to 0 (false).
//   - If server accepts a connection with CleanSession set to 0 SP depends on whether the server
//     already has stored a Session state for the supplied Client ID. If the server has stored a Session
//     then SP MUST set to 1, else MUST set to 0.
//
// In both cases above this is in addition to returning a zero CONNACK return code. If the CONNACK return code
// is non-zero then SP MUST set to 0.
func (vc VariablesConnack) SessionPresent() bool { return vc.AckFlags&1 != 0 }

// validate provides early validation of CONNACK variables.
func (vc VariablesConnack) validate() error {
	if vc.AckFlags&^1 != 0 {
		return errors.New("CONNACK Ack flag bits 7-1 must be set to 0")
	}
	return nil
}

// ConnectReturnCode defined in definitions.go

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

// DecodeHeader receives transp, an io.ByteReader that reads from an underlying arbitrary
// transport protocol. transp should start returning the first byte of the MQTT packet.
// Decode header returns the decoded header and any error that prevented it from
// reading the entire header as specified by the MQTT v3.1 protocol.
// It performs the minimal validating to ensure it does not over-read the header's contents.
// Header.Validate() should be called after DecodeHeader for a complete validation.
func DecodeHeader(transp io.Reader) (Header, int, error) {
	// Start parsing fixed header.
	firstByte, err := decodeByte(transp)
	if err != nil {
		return Header{}, 0, err
	}
	n := 1
	rlen, ngot, err := decodeRemainingLength(transp)
	n += ngot
	if err != nil {
		return Header{}, n, err
	}
	packetType := PacketType(firstByte >> 4)
	packetFlags := PacketFlags(firstByte & 0b1111)
	if err := packetType.ValidateFlags(packetFlags); err != nil {
		// Early validation to prevent reading more than necessary from buffer.
		return Header{}, n, err
	}
	hdr := Header{
		firstByte:       firstByte,
		RemainingLength: rlen,
	}
	return hdr, n, nil
}

// mqttStringSize returns the size on wire occupied
// by an *OPTIONAL* MQTT encoded string. If string is zero length returns 0.
func mqttStringSize(b []byte) int {
	lb := len(b)
	if lb > 0 {
		return lb + 2
	}
	return 0
}
