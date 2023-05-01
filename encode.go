package mqtt

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"math"
)

// Encode encodes the header into the argument writer. It will encode up to a maximum
// of 7 bytes, which is the max length header in MQTT v3.1.
func (hdr Header) Encode(w io.Writer) (n int, err error) {
	if hdr.RemainingLength > maxRemainingLengthValue {
		return 0, errors.New("remaining length too large for MQTT v3.1.1 spec")
	}
	var headerBuf [5]byte
	n = hdr.Put(headerBuf[:])
	return writeFull(w, headerBuf[:n])
}

func (hdr Header) Put(buf []byte) int {
	_ = buf[4]
	buf[0] = hdr.firstByte
	return encodeRemainingLength(hdr.RemainingLength, buf[1:]) + 1
}

func encodeMQTTString(w io.Writer, s []byte) (int, error) {
	length := len(s)
	if length == 0 {
		return 0, errors.New("cannot encode MQTT string of length 0")
	}
	if length > math.MaxUint16 {
		return 0, errors.New("cannot encode MQTT string of length > MaxUint16 or length 0")
	}
	n, err := encodeUint16(w, uint16(len(s)))
	if err != nil {
		return n, err
	}
	n2, err := writeFull(w, s)
	n += n2
	if err != nil {
		return n, err
	}
	return n, nil
}

// encodeRemainingLength encodes between 1 to 4 bytes.
func encodeRemainingLength(remlen uint32, b []byte) (n int) {
	if remlen > maxRemainingLengthValue {
		panic("remaining length too large. " + bugReportLink)
	}
	if remlen < 128 {
		// Fast path for small remaining lengths. Also the implementation below is not correct for remaining length = 0.
		b[0] = byte(remlen)
		return 1
	}

	for n = 0; remlen > 0; n++ {
		encoded := byte(remlen % 128)
		remlen /= 128
		if remlen > 0 {
			encoded |= 128
		}
		b[n] = encoded
	}
	return n
}

// All encode{PacketType} functions encode only their variable header.

// encodeConnect encodes a CONNECT packet variable header over w given connVars. Does not encode
// either the fixed header or the Packet Payload.
func encodeConnect(w io.Writer, varConn *VariablesConnect) (n int, err error) {
	// Begin encoding variable header buffer.
	var varHeaderBuf [10]byte
	// Set protocol name 'MQTT' and protocol level 4.
	n += copy(varHeaderBuf[:], "\x00\x04MQTT\x04") // writes 7 bytes.
	varHeaderBuf[n] = varConn.Flags()
	varHeaderBuf[n+1] = byte(varConn.KeepAlive >> 8) // MSB
	varHeaderBuf[n+2] = byte(varConn.KeepAlive)      // LSB
	// n+=3 // We've written 10 bytes exactly if all went well up to here.
	n, err = w.Write(varHeaderBuf[:])
	if err == nil && n != 10 {
		return n, errors.New("single write did not complete for encoding, use larger underlying buffer")
	}
	if err != nil {
		return n, err
	}
	// Begin Encoding payload contents. First field is ClientID.
	ngot, err := encodeMQTTString(w, varConn.ClientID)
	n += ngot
	if err != nil {
		return n, err
	}

	if varConn.WillFlag() {
		ngot, err = encodeMQTTString(w, varConn.WillTopic)
		n += ngot
		if err != nil {
			return n, err
		}
		ngot, err = encodeMQTTString(w, varConn.WillMessage)
		n += ngot
		if err != nil {
			return n, err
		}
	}

	if len(varConn.Username) != 0 {
		// Username and password.
		ngot, err = encodeMQTTString(w, varConn.Username)
		n += ngot
		if err != nil {
			return n, err
		}
		if len(varConn.Password) != 0 {
			ngot, err = encodeMQTTString(w, varConn.Password)
			n += ngot
			if err != nil {
				return n, err
			}
		}
	}
	return n, nil
}

func encodeConnack(w io.Writer, varConn VariablesConnack) (int, error) {
	var buf [2]byte
	buf[0] = varConn.AckFlags
	buf[1] = byte(varConn.ReturnCode)
	return writeFull(w, buf[:])
}

// encodePublish encodes PUBLISH packet variable header. Does not encode fixed header or user payload.
func encodePublish(w io.Writer, qos QoSLevel, varPub VariablesPublish) (n int, err error) {
	n, err = encodeMQTTString(w, varPub.TopicName)
	if err != nil {
		return n, err
	}
	if qos != QoS0 {
		ngot, err := encodeUint16(w, varPub.PacketIdentifier)
		n += ngot
		if err != nil {
			return n, err
		}
	}
	return n, err
}

func encodeByte(w io.Writer, value byte) (n int, err error) {
	var vbuf [1]byte
	vbuf[0] = value
	return w.Write(vbuf[:])
}

func encodeUint16(w io.Writer, value uint16) (n int, err error) {
	var vbuf [2]byte
	binary.BigEndian.PutUint16(vbuf[:], value)
	return writeFull(w, vbuf[:])
}

func encodeSubscribe(w io.Writer, varSub VariablesSubscribe) (n int, err error) {
	if len(varSub.TopicFilters) == 0 {
		return 0, errors.New("payload of SUBSCRIBE must contain at least one topic filter / QoS pair")
	}
	n, err = encodeUint16(w, varSub.PacketIdentifier)
	if err != nil {
		return n, err
	}
	var vbuf [1]byte
	for _, hotTopic := range varSub.TopicFilters {
		ngot, err := encodeMQTTString(w, hotTopic.TopicFilter)
		n += ngot
		if err != nil {
			return n, err
		}
		vbuf[0] = byte(hotTopic.QoS & 0b11)
		ngot, err = w.Write(vbuf[:1])
		n += ngot
		if err != nil {
			return n, err
		}
	}
	return n, nil
}

func encodeSuback(w io.Writer, varSuback VariablesSuback) (n int, err error) {
	n, err = encodeUint16(w, varSuback.PacketIdentifier)
	if err != nil {
		return n, err
	}
	for _, qos := range varSuback.ReturnCodes {
		if !qos.IsValid() && qos != QoSSubfail { // Suback can encode a subfail.
			panic("encodeSuback received an invalid QoS return code. " + bugReportLink)
		}
		ngot, err := encodeByte(w, byte(qos))
		n += ngot
		if err != nil {
			return n, err
		}
	}
	return n, nil
}

func encodeUnsubscribe(w io.Writer, varUnsub VariablesUnsubscribe) (n int, err error) {
	if len(varUnsub.Topics) == 0 {
		return 0, errors.New("payload of UNSUBSCRIBE must contain at least one topic")
	}
	n, err = encodeUint16(w, varUnsub.PacketIdentifier)
	if err != nil {
		return n, err
	}
	for _, coldTopic := range varUnsub.Topics {
		ngot, err := encodeMQTTString(w, coldTopic)
		n += ngot
		if err != nil {
			return n, err
		}
	}
	return n, nil
}

// Pings and DISCONNECT do not have variable headers so no encoders here.

func writeFull(dst io.Writer, src []byte) (int, error) {
	// dataPtr := 0
	n, err := dst.Write(src)
	if err == nil && n != len(src) {
		// TODO(soypat): Avoid heavy heap allocation by implementing lightweight algorithm here.
		var buffer [256]byte
		i, err := io.CopyBuffer(dst, bytes.NewBuffer(src[n:]), buffer[:])
		return n + int(i), err
	}
	return n, err
}

// bool to uint8
//
//go:inline
func b2u8(b bool) uint8 {
	if b {
		return 1
	}
	return 0
}
