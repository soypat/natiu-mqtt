package mqtt

import (
	"errors"
	"io"
	"math"
)

// Encode encodes the header into the argument writer. It will encode up to a maximum
// of 7 bytes, which is the max length header in MQTT v3.1.
func (hdr Header) Encode(w io.Writer) (n int, err error) {
	var headerBuf [7]byte
	headerBuf[0] = hdr.firstByte
	n = encodeRemainingLength(hdr.RemainingLength, headerBuf[1:])
	if hdr.PacketIdentifier != 0 {
		headerBuf[n+1] = byte(hdr.PacketIdentifier >> 8)
		headerBuf[n+2] = byte(hdr.PacketIdentifier)
		n += 2
	}
	nwritten, err := w.Write(headerBuf[:n])
	if err == nil && nwritten != n {
		return nwritten, errors.New("single write did not complete for Header.Encode, use larger underlying buffer")
	}
	return nwritten, err
}

// All encode{PacketType} functions encode only their variable header.

// encodeConnect encodes a CONNECT packet variable header over w given connVars. Does not encode
// either the fixed header or the Packet Payload.
func encodeConnect(w io.Writer, varConn VariablesConnect) (n int, err error) {
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

	if varConn.Username != "" {
		// Username and password.
		ngot, err = encodeMQTTString(w, varConn.Username)
		n += ngot
		if err != nil {
			return n, err
		}
		if varConn.Password != "" {
			ngot, err = encodeMQTTString(w, varConn.Password)
			n += ngot
			if err != nil {
				return n, err
			}
		}
	}
	return n, nil
}

// encodePublish encodes PUBLISH packet variable header. Does not encode fixed header or user payload.
func encodePublish(w io.Writer, varPub VariablesPublish) (n int, err error) {
	var vbuf [2]byte
	vbuf[0] = byte(varPub.PacketIdentifier >> 8)
	vbuf[1] = byte(varPub.PacketIdentifier)
	n, err = encodeMQTTString(w, varPub.TopicName)
	if err != nil {
		return n, err
	}
	return writeFull(w, vbuf[:])
}

// encodePublishResponse encodes a PUBACK, PUBREL, PUBREC, PUBCOMP packet. Does not encode fixed header or user payload.
func encodePublishResponse(w io.Writer, packetIdentifier uint16) (n int, err error) {
	var vbuf [2]byte
	vbuf[0] = byte(packetIdentifier >> 8)
	vbuf[1] = byte(packetIdentifier)
	return writeFull(w, vbuf[:])
}

func encodeSubscribe(w io.Writer, varSub VariablesSubscribe) (n int, err error) {
	if len(varSub.TopicFilters) == 0 {
		return 0, errors.New("payload of SUBSCRIBE must contain at least one topic filter / QoS pair")
	}
	var vbuf [2]byte
	vbuf[0] = byte(varSub.PacketIdentifier >> 8)
	vbuf[1] = byte(varSub.PacketIdentifier)
	n, err = writeFull(w, vbuf[:])
	if err != nil {
		return n, err
	}
	for _, sub := range varSub.TopicFilters {
		ngot, err := encodeMQTTString(w, sub.Topic)
		n += ngot
		if err != nil {
			return n, err
		}
		vbuf[0] = byte(sub.QoS & 0b11)
		ngot, err = w.Write(vbuf[:1])
		n += ngot
		if err != nil {
			return n, err
		}
	}
	return n, nil
}

func encodeSuback(w io.Writer)

func writeFull(w io.Writer, rawData []byte) (int, error) {
	// dataPtr := 0
	n, err := w.Write(rawData)
	if err == nil && n != len(rawData) {
		panic("TODO")
		return n, errors.New("TODO IMPLEMENT THIS")
	}
	return n, err
}

func encodeMQTTString(w io.Writer, s string) (int, error) {
	length := len(s)
	if length > math.MaxUint16 {
		return 0, errors.New("cannot encode MQTT string of length > MaxUint16")
	}
	var lengthBuf [2]byte
	lengthBuf[0] = byte(length >> 8) // MSB
	lengthBuf[1] = byte(length)      // MSB
	n, err := writeFull(w, lengthBuf[:])
	if err != nil {
		return n, err
	}
	n2, err := writeFull(w, bytesFromString(s))
	if err != nil {
		return n + n2, err
	}
	return n + n2, nil
}
