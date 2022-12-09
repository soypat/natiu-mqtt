package mqtt

import (
	"bytes"
	"errors"
	"io"
	"strconv"
)

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
	packetType := PacketType(firstByte >> 4)
	packetFlags := PacketFlags(firstByte & 0b1111)
	// TODO(soypat): Should this validation be performed here?
	if err := packetType.ValidateFlags(packetFlags); err != nil {
		return Header{}, n, err
	}
	var PI uint16
	if packetType.containsPacketIdentifier(packetFlags) {
		pi, ngot, err := decodeUint16(transp)
		n += ngot
		if err != nil {
			return Header{}, n, err
		}
		if pi == 0 {
			return Header{}, n, errGotZeroPI
		}
		PI = pi
	}
	hdr := Header{
		firstByte:        firstByte,
		RemainingLength:  rlen,
		PacketIdentifier: PI,
	}
	return hdr, n, nil
}

// decodeRemainingLength decodes the Remaining Length variable length integer
// in MQTT fixed headers. This value can range from 1 to 4 bytes in length and
func decodeRemainingLength(r io.Reader) (value uint32, n int, err error) {
	multiplier := uint32(1)
	for i := 0; i < maxRemainingLengthSize; i++ {
		encodedByte, err := decodeByte(r)
		if err != nil {
			return value, n, err
		}
		n++
		value += uint32(encodedByte&127) * multiplier
		if encodedByte&128 != 0 {
			return value, n, nil
		}
		multiplier *= 128
	}
	return 0, n, errors.New("malformed remaining length")
}

func readFull(src io.Reader, dst []byte) (int, error) {
	n, err := src.Read(dst)
	if err == nil && n != len(dst) {
		var buffer [256]byte
		// TODO(soypat): Avoid heavy heap allocation by implementing lightweight algorithm here.
		i64, err := io.CopyBuffer(bytes.NewBuffer(dst[n:]), src, buffer[:])
		i := int(i64)
		if err != nil && errors.Is(err, io.EOF) && i == len(dst[n:]) {
			err = nil
		}
		return n + i, err
	}
	return n, err
}

// decodeMQTT unmarshals a string from r into buffer's start. The unmarshalled
// string can be at most len(buffer). buffer must be at least of length 2.
// decodeMQTTString only returns a non-nil string on a succesfull decode.
func decodeMQTTString(r io.Reader, buffer []byte) ([]byte, int, error) {
	if len(buffer) < 2 {
		return nil, 0, errors.New("buffer too small for string decoding (<2)")
	}
	stringLength, n, err := decodeUint16(r)
	if err != nil {
		return nil, n, err
	}
	if stringLength == 0 {
		return nil, n, errors.New("zero length MQTT string")
	}
	if stringLength > uint16(len(buffer)) {
		return nil, n, errors.New("buffer too small for string of length " + strconv.FormatUint(uint64(stringLength), 10))
	}
	ngot, err := readFull(r, buffer[:stringLength])
	n += ngot
	if err != nil && errors.Is(err, io.EOF) && uint16(ngot) == stringLength {
		err = nil // MQTT string was read succesfully albeit with an EOF right at the end.
	}
	return buffer[:stringLength], n, err
}

func decodeByte(r io.Reader) (value byte, err error) {
	var vbuf [1]byte
	n, err := r.Read(vbuf[:])
	if err != nil && errors.Is(err, io.EOF) && n == 1 {
		err = nil // Byte was read succesfully albeit with an EOF.
	}
	return vbuf[0], err
}

func decodeUint16(r io.Reader) (value uint16, n int, err error) {
	var vbuf [2]byte
	n, err = readFull(r, vbuf[:])
	if err != nil && errors.Is(err, io.EOF) && n == 2 {
		err = nil // integer was read succesfully albeit with an EOF.
	}
	return uint16(vbuf[0])<<8 | uint16(vbuf[1]), n, err
}

func decodeConnack(r io.Reader) (VariablesConnack, int, error) {
	var buf [2]byte
	n, err := readFull(r, buf[:])
	if err != nil {
		return VariablesConnack{}, n, err
	}
	varConnack := VariablesConnack{AckFlags: buf[0], ReturnCode: ConnectReturnCode(buf[1])}
	if err = varConnack.validate(); err != nil {
		return VariablesConnack{}, n, err
	}
	return varConnack, n, nil
}

func decodePublish(r io.Reader, payloadDst []byte, qos QoSLevel) (VariablesPublish, int, error) {
	topic, n, err := decodeMQTTString(r, payloadDst)
	if err != nil {
		return VariablesPublish{}, n, err
	}
	var PI uint16
	if qos == 1 || qos == 2 {
		pi, ngot, err := decodeUint16(r)
		n += ngot
		if err != nil { // && !errors.Is(err, io.EOF) TODO(soypat): Investigate if it is necessary to guard against io.EOFs on packet ends.
			return VariablesPublish{}, n, err
		}
		PI = pi
	}
	// TODO(soypat): Right now we do not use payloadDst efficiently. We copy to heap here with string() call.
	// Consider changing Variable{PacketName} contents to be []byte types instead of strings.
	return VariablesPublish{TopicName: string(topic), PacketIdentifier: PI}, n, nil
}

func decodePublishResponse(r io.Reader) (uint16, int, error) {
	return decodeUint16(r)
}

func decodeSubscribe(r io.Reader, buffer []byte, remainingLen uint32) (varSub VariablesSubscribe, n int, err error) {
	if len(varSub.TopicFilters) == 0 {
		return VariablesSubscribe{}, 0, errors.New("payload of SUBSCRIBE must contain at least one topic filter / QoS pair")
	}
	varSub.PacketIdentifier, n, err = decodeUint16(r)
	if err != nil {
		return VariablesSubscribe{}, n, err
	}
	for n < int(remainingLen) {
		hotTopic, ngot, err := decodeMQTTString(r, buffer)
		n += ngot
		if err != nil {
			return VariablesSubscribe{}, n, err
		}
		qos, err := decodeByte(r)
		if err != nil {
			return VariablesSubscribe{}, n, err
		}
		n++
		varSub.TopicFilters = append(varSub.TopicFilters, SubscribeRequest{Topic: string(hotTopic), QoS: QoSLevel(qos)})
	}
	return varSub, n, nil
}

func decodeSuback(r io.Reader, remainingLen uint16) (varSuback VariablesSuback, n int, err error) {
	varSuback.PacketIdentifier, n, err = decodeUint16(r)
	if err != nil {
		return VariablesSuback{}, n, err
	}
	for n < int(remainingLen) {
		qos, err := decodeByte(r)
		if err != nil {
			return VariablesSuback{}, n, err
		}
		n++
		varSuback.ReturnCodes = append(varSuback.ReturnCodes, QoSLevel(qos))
	}
	return varSuback, n, nil
}
