package mqtt

import (
	"errors"
	"io"
)

// RxTx implements a bare minimum MQTT protocol transport layer handler.
type RxTx struct {
	// LastReceivedHeader contains the last correctly read header.
	LastReceivedHeader Header
	// Functions below can access the Header of the message via RxTx.LastReceivedHeader.
	// All these functions block RxTx.ReadNextPacket.
	OnConnect func(*RxTx, *VariablesConnect) error // Receives pointer because of large struct!
	OnConnack func(*RxTx, VariablesConnack) error
	OnPub     func(*RxTx, VariablesPublish, io.Reader) error
	// OnOther takes in the Header of received packet and a packet identifier uint16 if present.
	// OnOther receives PUBACK, PUBREC, PUBREL, PUBCOMP, UNSUBACK packets containing non-zero packet identfiers
	// and DISCONNECT, PINGREQ, PINGRESP packets with no packet identifier.
	OnOther  func(rxtx *RxTx, packetIdentifier uint16) error
	OnSub    func(*RxTx, VariablesSubscribe) error
	OnSuback func(*RxTx, VariablesSuback) error
	OnUnsub  func(*RxTx, VariablesUnsubscribe) error
	// Transport over which packets are read and written to.
	// Not exported since RxTx type might be composed of embedded Rx and Tx types in future. TBD.
	trp io.ReadWriteCloser
	// User defined decoder for allocating packets.
	userDecoder Decoder
	// Default decoder for non allocating packets.
	dec        DecoderLowmem
	ScratchBuf []byte
}

// NewRxTx creates a new RxTx. Before use user must configure OnX fields by setting a function
// to perform an action each time a packet is received. After a call to transport.Close()
// all future calls must return errors until the transport is replaced with [RxTx.SetTransport].
func NewRxTx(transport io.ReadWriteCloser, decoder Decoder) (*RxTx, error) {
	if transport == nil || decoder == nil {
		return nil, errors.New("got nil transport io.ReadWriteCloser or nil Decoder")
	}
	cc := &RxTx{
		trp:         transport,
		userDecoder: decoder,
		// No memory needed for DecoderLowmem for this use.
		dec: DecoderLowmem{},
	}
	return cc, nil
}

// Close closes the underlying transport.
func (rxtx *RxTx) Close() error { return rxtx.trp.Close() }

// SetTransport sets the rxtx's reader and writer.
func (rxtx *RxTx) SetTransport(transport io.ReadWriteCloser) {
	rxtx.trp = transport
}

// ReadNextPacket reads the next packet in the transport. If it fails it closes the transport
// and the underlying transport must be reset.
func (rxtx *RxTx) ReadNextPacket() (int, error) {
	hdr, n, err := DecodeHeader(rxtx.trp)
	if err != nil {
		rxtx.Close()
		return n, err
	}
	rxtx.LastReceivedHeader = hdr
	var (
		ngot             int
		packetIdentifier uint16
	)
	switch hdr.Type() {
	case PacketPublish:
		var vp VariablesPublish
		vp, ngot, err = rxtx.userDecoder.DecodePublish(rxtx.trp, hdr.Flags().QoS())
		n += ngot
		if err != nil {
			break
		}
		payloadLen := int(hdr.RemainingLength) - ngot
		lr := io.LimitedReader{R: rxtx.trp, N: int64(payloadLen)}
		if rxtx.OnPub != nil {
			err = rxtx.OnPub(rxtx, vp, &lr)
		} else {
			err = rxtx.exhaustReader(&lr)
		}

		if lr.N != 0 && err == nil {
			err = errors.New("expected OnPub to completely read payload")
			break
		}

	case PacketConnack:
		var vc VariablesConnack
		vc, ngot, err = rxtx.dec.DecodeConnack(rxtx.trp)
		n += ngot
		if err != nil {
			break
		}
		if rxtx.OnConnack != nil {
			err = rxtx.OnConnack(rxtx, vc)
		}

	case PacketConnect:
		var vc VariablesConnect
		vc, ngot, err = rxtx.userDecoder.DecodeConnect(rxtx.trp)
		n += ngot
		if err != nil {
			break
		}
		if rxtx.OnConnect != nil {
			err = rxtx.OnConnect(rxtx, &vc)
		}

	case PacketSuback:
		var vsbck VariablesSuback
		vsbck, ngot, err = rxtx.dec.DecodeSuback(rxtx.trp, hdr.RemainingLength)
		n += ngot
		if err != nil {
			break
		}
		if rxtx.OnSuback != nil {
			err = rxtx.OnSuback(rxtx, vsbck)
		}

	case PacketSubscribe:
		var vsbck VariablesSubscribe
		vsbck, ngot, err = rxtx.userDecoder.DecodeSubscribe(rxtx.trp, hdr.RemainingLength)
		n += ngot
		if err != nil {
			break
		}
		if rxtx.OnSub != nil {
			err = rxtx.OnSub(rxtx, vsbck)
		}

	case PacketUnsubscribe:
		var vunsub VariablesUnsubscribe
		vunsub, ngot, err = rxtx.userDecoder.DecodeUnsubscribe(rxtx.trp, hdr.RemainingLength)
		n += ngot
		if err != nil {
			break
		}
		if rxtx.OnUnsub != nil {
			err = rxtx.OnUnsub(rxtx, vunsub)
		}

	case PacketPuback, PacketPubrec, PacketPubrel, PacketPubcomp, PacketUnsuback:
		// Only PI, no payload.
		pi, ngot, err := decodeUint16(rxtx.trp)
		n += ngot
		if err != nil {
			break
		}
		packetIdentifier = pi
		fallthrough
	case PacketDisconnect, PacketPingreq, PacketPingresp:
		// No payload or variable header.
		if rxtx.OnOther != nil {
			err = rxtx.OnOther(rxtx, packetIdentifier)
		}

	default:
		// Header Decode should return an error on incorrect package type receive.
		// This could be tested via fuzzing.
		panic("unreachable")
	}

	if err != nil {
		rxtx.Close()
	}
	return n, err
}

func (rxtx *RxTx) exhaustReader(r io.Reader) (err error) {
	if len(rxtx.ScratchBuf) == 0 {
		rxtx.ScratchBuf = make([]byte, 1024) // Lazy initialization when needed.
	}
	for err == nil {
		_, err = r.Read(rxtx.ScratchBuf[:])
	}
	if errors.Is(err, io.EOF) {
		return nil
	}
	return err
}

// WriteConnack writes a CONNECT packet over the transport.
func (rxtx *RxTx) WriteConnect(varConn *VariablesConnect) error {
	h := newHeader(PacketConnect, 0, uint32(varConn.Size()))
	_, err := h.Encode(rxtx.trp)
	if err != nil {
		return err
	}
	_, err = encodeConnect(rxtx.trp, varConn)
	return err
}

// WriteConnack writes a CONNACK packet over the transport.
func (rxtx *RxTx) WriteConnack(varConnack VariablesConnack) error {
	h := newHeader(PacketConnack, 0, uint32(varConnack.Size()))
	_, err := h.Encode(rxtx.trp)
	if err != nil {
		return err
	}
	_, err = encodeConnack(rxtx.trp, varConnack)
	return err
}

// WritePublishPayload writes a PUBLISH packet over the transport along with the
// Application Message in the payload. payload can be zero-length.
func (rxtx *RxTx) WritePublishPayload(h Header, varPub VariablesPublish, payload []byte) error {
	h.RemainingLength = uint32(varPub.Size() + len(payload))
	_, err := h.Encode(rxtx.trp)
	if err != nil {
		return err
	}
	_, err = encodePublish(rxtx.trp, varPub)
	if err != nil {
		return err
	}
	_, err = writeFull(rxtx.trp, payload)
	return err
}

// WriteSubscribe writes an SUBSCRIBE packet over the transport.
func (rxtx *RxTx) WriteSubscribe(varSub VariablesSubscribe) error {
	h := newHeader(PacketSubscribe, PacketFlagsPubrelSubUnsub, uint32(varSub.Size()))
	_, err := h.Encode(rxtx.trp)
	if err != nil {
		return err
	}
	_, err = encodeSubscribe(rxtx.trp, varSub)
	return err
}

// WriteSuback writes an UNSUBACK packet over the transport.
func (rxtx *RxTx) WriteSuback(varSub VariablesSuback) error {
	h := newHeader(PacketSuback, 0, uint32(varSub.Size()))
	_, err := h.Encode(rxtx.trp)
	if err != nil {
		return err
	}
	_, err = encodeSuback(rxtx.trp, varSub)
	return err
}

// WriteUnsubscribe writes an UNSUBSCRIBE packet over the transport.
func (rxtx *RxTx) WriteUnsubscribe(varUnsub VariablesUnsubscribe) error {
	h := newHeader(PacketUnsubscribe, PacketFlagsPubrelSubUnsub, uint32(varUnsub.Size()))
	_, err := h.Encode(rxtx.trp)
	if err != nil {
		return err
	}
	_, err = encodeUnsubscribe(rxtx.trp, varUnsub)
	return err
}

// WriteOther writes PUBACK, PUBREC, PUBREL, PUBCOMP, UNSUBACK packets containing non-zero packet identfiers
// and DISCONNECT, PINGREQ, PINGRESP packets with no packet identifier. It automatically sets the RemainingLength field.
func (rxtx *RxTx) WriteOther(h Header, packetIdentifier uint16) (err error) {
	hasPI := h.HasPacketIdentifier()
	if hasPI {
		h.RemainingLength = 2
		_, err = h.Encode(rxtx.trp)
		if err != nil {
			return err
		}
		_, err = encodeUint16(rxtx.trp, packetIdentifier)
	} else {
		h.RemainingLength = 0
		_, err = h.Encode(rxtx.trp)
	}
	return err
}
