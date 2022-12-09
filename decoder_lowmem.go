package mqtt

import (
	"bytes"
	"errors"
	"io"
)

// DecoderLowmem implements the Decoder interface for unmarshalling Variable headers
// of MQTT packets. This particular implementation avoids heap allocations to ensure
// minimal memory usage during decoding. The UserBuffer is used up to it's length.
// Needless to say, this implementation is NOT safe for concurrent use.
type DecoderLowmem struct {
	UserBuffer []byte
}

func (d DecoderLowmem) DecodeConnect(r io.Reader) (varConn VariablesConnect, n int, err error) {
	payloadDst := d.UserBuffer
	var ngot int
	varConn.Protocol, n, err = decodeMQTTString(r, payloadDst)
	if err != nil {
		return VariablesConnect{}, n, err
	}
	payloadDst = payloadDst[n:]
	varConn.ProtocolLevel, err = decodeByte(r)
	if err != nil {
		return VariablesConnect{}, n, err
	}
	n++
	flags, err := decodeByte(r)
	if err != nil {
		return VariablesConnect{}, n, err
	}
	n++
	if flags&1 != 0 { // [MQTT-3.1.2-3].
		return VariablesConnect{}, n, errors.New("reserved bit set in CONNECT flag")
	}
	userNameFlag := flags&(1<<7) != 0
	passwordFlag := flags&(1<<6) != 0
	varConn.WillRetain = flags&(1<<5) != 0
	varConn.WillQoS = QoSLevel(flags>>3) & 0b11
	willFlag := flags&(1<<2) != 0
	varConn.CleanSession = flags&(1<<1) != 0
	if passwordFlag && !userNameFlag {
		return VariablesConnect{}, n, errors.New("username flag must be set to use password flag")
	}

	varConn.KeepAlive, ngot, err = decodeUint16(r)
	n += ngot
	if err != nil {
		return VariablesConnect{}, n, err
	}
	varConn.ClientID, ngot, err = decodeMQTTString(r, payloadDst)
	if err != nil {
		return VariablesConnect{}, n, err
	}
	payloadDst = payloadDst[ngot:]

	if willFlag {
		varConn.WillTopic, ngot, err = decodeMQTTString(r, payloadDst)
		n += ngot
		if err != nil {
			return VariablesConnect{}, n, err
		}
		payloadDst = payloadDst[ngot:]
		varConn.WillMessage, ngot, err = decodeMQTTString(r, payloadDst)
		n += ngot
		if err != nil {
			return VariablesConnect{}, n, err
		}
		payloadDst = payloadDst[ngot:]
	}

	if userNameFlag {
		// Username and password.
		varConn.Username, ngot, err = decodeMQTTString(r, payloadDst)
		n += ngot
		if err != nil {
			return VariablesConnect{}, n, err
		}
		if passwordFlag {
			payloadDst = payloadDst[ngot:]
			varConn.Password, ngot, err = decodeMQTTString(r, payloadDst)
			n += ngot
			if err != nil {
				return VariablesConnect{}, n, err
			}
		}
	}
	return varConn, n, nil
}

func (d DecoderLowmem) DecodeConnack(r io.Reader) (VariablesConnack, int, error) {
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

// DecodePublish Decodes PUBLISH variable header.
func (d DecoderLowmem) DecodePublish(r io.Reader, qos QoSLevel) (VariablesPublish, int, error) {
	topic, n, err := decodeMQTTString(r, d.UserBuffer)
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
	return VariablesPublish{TopicName: topic, PacketIdentifier: PI}, n, nil
}

func (d DecoderLowmem) DecodeSubscribe(r io.Reader, remainingLen uint32) (varSub VariablesSubscribe, n int, err error) {
	payloadDst := d.UserBuffer
	varSub.PacketIdentifier, n, err = decodeUint16(r)
	if err != nil {
		return VariablesSubscribe{}, n, err
	}
	for n < int(remainingLen) {
		hotTopic, ngot, err := decodeMQTTString(r, payloadDst)
		n += ngot
		payloadDst = payloadDst[ngot:] //Advance buffer pointer to not overwrite.
		if err != nil {
			return VariablesSubscribe{}, n, err
		}
		qos, err := decodeByte(r)
		if err != nil {
			return VariablesSubscribe{}, n, err
		}
		n++
		varSub.TopicFilters = append(varSub.TopicFilters, SubscribeRequest{TopicFilter: hotTopic, QoS: QoSLevel(qos)})
	}
	return varSub, n, nil
}

func (d DecoderLowmem) DecodeSuback(r io.Reader, remainingLen uint32) (varSuback VariablesSuback, n int, err error) {
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

func (d DecoderLowmem) DecodeUnsubscribe(r io.Reader, remainingLength uint32) (varUnsub VariablesUnsubscribe, n int, err error) {
	payloadDst := d.UserBuffer
	varUnsub.PacketIdentifier, n, err = decodeUint16(r)
	if err != nil {
		return VariablesUnsubscribe{}, n, err
	}
	for n < int(remainingLength) {
		coldTopic, ngot, err := decodeMQTTString(r, payloadDst)
		n += ngot
		payloadDst = payloadDst[ngot:] // Advance buffer pointer to not overwrite.
		if err != nil {
			return VariablesUnsubscribe{}, n, err
		}
		varUnsub.Topics = append(varUnsub.Topics, coldTopic)
	}
	return varUnsub, n, nil
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
		return nil, 0, ErrUserBufferFull
	}
	stringLength, n, err := decodeUint16(r)
	if err != nil {
		return nil, n, err
	}
	if stringLength == 0 {
		return nil, n, errors.New("zero length MQTT string")
	}
	if stringLength > uint16(len(buffer)) {
		return nil, n, ErrUserBufferFull // errors.New("buffer too small for string of length " + strconv.FormatUint(uint64(stringLength), 10))
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
