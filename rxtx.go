package mqtt

import (
	"errors"
	"io"
)

// RxTx implements a bare minimum MQTT v3.1.1 protocol transport layer handler.
// If there is an error during read/write of a packet the transport is closed
// and a new transport must be set with [RxTx.SetTransport].
// An RxTx will not validate data before encoding, that is up to the caller, it
// will validate incoming data according to MQTT's specification. Malformed packets
// will be rejected and the connection will be closed immediately with a call to [RxTx.OnError].
type RxTx struct {
	Tx
	Rx
}

// ShallowCopy copies RxTx. Does not copy callbacks over.
func (rxtx *RxTx) ShallowCopy() *RxTx {
	return &RxTx{
		Tx: Tx{txTrp: rxtx.txTrp},
		Rx: Rx{rxTrp: rxtx.Rx.rxTrp, userDecoder: rxtx.Rx.userDecoder},
	}
}

type Rx struct {
	// LastReceivedHeader contains the last correctly read header.
	LastReceivedHeader Header
	// Transport over which packets are read and written to.
	// Not exported since RxTx type might be composed of embedded Rx and Tx types in future. TBD.
	rxTrp io.ReadCloser
	// Functions below can access the Header of the message via RxTx.LastReceivedHeader.
	// All these functions block RxTx.ReadNextPacket.
	OnConnect func(*Rx, *VariablesConnect) error // Receives pointer because of large struct!
	OnConnack func(*Rx, VariablesConnack) error
	OnPub     func(*Rx, VariablesPublish, io.Reader) error
	// OnOther takes in the Header of received packet and a packet identifier uint16 if present.
	// OnOther receives PUBACK, PUBREC, PUBREL, PUBCOMP, UNSUBACK packets containing non-zero packet identfiers
	// and DISCONNECT, PINGREQ, PINGRESP packets with no packet identifier.
	OnOther  func(rx *Rx, packetIdentifier uint16) error
	OnSub    func(*Rx, VariablesSubscribe) error
	OnSuback func(*Rx, VariablesSuback) error
	OnUnsub  func(*Rx, VariablesUnsubscribe) error
	// OnRxError is called if an error is encountered during encoding or decoding of packet.
	OnRxError func(error)

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
		Rx: Rx{
			rxTrp:       transport,
			userDecoder: decoder,
		},
		Tx: Tx{txTrp: transport},
	}
	return cc, nil
}

// Close closes the underlying transport.
func (rxtx *Rx) Close() error { return rxtx.rxTrp.Close() }
func (rxtx *Rx) prepClose(err error) {
	if rxtx.OnRxError != nil {
		rxtx.OnRxError(err)
	}
	err = rxtx.Close()
	if rxtx.OnRxError != nil {
		rxtx.OnRxError(err)
	}
}

// SetTransport sets the rxtx's reader and writer.
func (rxtx *Rx) SetTransport(transport io.ReadWriteCloser) {
	rxtx.rxTrp = transport
}

// ReadNextPacket reads the next packet in the transport. If it fails it closes the transport
// and the underlying transport must be reset.
func (rxtx *Rx) ReadNextPacket() (int, error) {
	if rxtx.rxTrp == nil {
		return 0, errors.New("nil transport")
	}
	hdr, n, err := DecodeHeader(rxtx.rxTrp)
	if err != nil {
		rxtx.prepClose(err)
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
		vp, ngot, err = rxtx.userDecoder.DecodePublish(rxtx.rxTrp, hdr.Flags().QoS())
		n += ngot
		if err != nil {
			break
		}
		payloadLen := int(hdr.RemainingLength) - ngot
		lr := io.LimitedReader{R: rxtx.rxTrp, N: int64(payloadLen)}
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
		vc, ngot, err = rxtx.dec.DecodeConnack(rxtx.rxTrp)
		n += ngot
		if err != nil {
			break
		}
		if rxtx.OnConnack != nil {
			err = rxtx.OnConnack(rxtx, vc)
		}

	case PacketConnect:
		var vc VariablesConnect
		vc, ngot, err = rxtx.userDecoder.DecodeConnect(rxtx.rxTrp)
		n += ngot
		if err != nil {
			break
		}
		if rxtx.OnConnect != nil {
			err = rxtx.OnConnect(rxtx, &vc)
		}

	case PacketSuback:
		var vsbck VariablesSuback
		vsbck, ngot, err = rxtx.dec.DecodeSuback(rxtx.rxTrp, hdr.RemainingLength)
		n += ngot
		if err != nil {
			break
		}
		if rxtx.OnSuback != nil {
			err = rxtx.OnSuback(rxtx, vsbck)
		}

	case PacketSubscribe:
		var vsbck VariablesSubscribe
		vsbck, ngot, err = rxtx.userDecoder.DecodeSubscribe(rxtx.rxTrp, hdr.RemainingLength)
		n += ngot
		if err != nil {
			break
		}
		if rxtx.OnSub != nil {
			err = rxtx.OnSub(rxtx, vsbck)
		}

	case PacketUnsubscribe:
		var vunsub VariablesUnsubscribe
		vunsub, ngot, err = rxtx.userDecoder.DecodeUnsubscribe(rxtx.rxTrp, hdr.RemainingLength)
		n += ngot
		if err != nil {
			break
		}
		if rxtx.OnUnsub != nil {
			err = rxtx.OnUnsub(rxtx, vunsub)
		}

	case PacketPuback, PacketPubrec, PacketPubrel, PacketPubcomp, PacketUnsuback:
		// Only PI, no payload.
		packetIdentifier, ngot, err = decodeUint16(rxtx.rxTrp)
		n += ngot
		if err != nil {
			break
		}
		fallthrough
	case PacketDisconnect, PacketPingreq, PacketPingresp:
		// No payload or variable header.
		if rxtx.OnOther != nil {
			err = rxtx.OnOther(rxtx, packetIdentifier)
		}

	default:
		// Header Decode should return an error on incorrect packet type receive.
		// This could be tested via fuzzing.
		panic("unreachable")
	}

	if err != nil {
		rxtx.prepClose(err)
	}
	return n, err
}

func (rxtx *Rx) exhaustReader(r io.Reader) (err error) {
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

// Tx implements
type Tx struct {
	txTrp          io.WriteCloser
	OnTxError      func(error)
	OnSuccessfulTx func()
}

// WriteConnack writes a CONNECT packet over the transport.
func (tx *Tx) WriteConnect(varConn *VariablesConnect) error {
	if tx.txTrp == nil {
		return errors.New("nil transport")
	}
	h := newHeader(PacketConnect, 0, uint32(varConn.Size()))
	_, err := h.Encode(tx.txTrp)
	if err != nil {
		tx.prepClose(err)
		return err
	}
	_, err = encodeConnect(tx.txTrp, varConn)
	if err != nil {
		tx.prepClose(err)
	}
	return err
}

// WriteConnack writes a CONNACK packet over the transport.
func (tx *Tx) WriteConnack(varConnack VariablesConnack) error {
	if tx.txTrp == nil {
		return errors.New("nil transport")
	}
	h := newHeader(PacketConnack, 0, uint32(varConnack.Size()))
	_, err := h.Encode(tx.txTrp)
	if err != nil {
		tx.prepClose(err)
		return err
	}
	_, err = encodeConnack(tx.txTrp, varConnack)
	if err != nil {
		tx.prepClose(err)
	}
	return err
}

// WritePublishPayload writes a PUBLISH packet over the transport along with the
// Application Message in the payload. payload can be zero-length.
func (tx *Tx) WritePublishPayload(h Header, varPub VariablesPublish, payload []byte) error {
	if tx.txTrp == nil {
		return errors.New("nil transport")
	}
	h.RemainingLength = uint32(varPub.Size() + len(payload))
	_, err := h.Encode(tx.txTrp)
	if err != nil {
		tx.prepClose(err)
		return err
	}
	_, err = encodePublish(tx.txTrp, varPub)
	if err != nil {
		tx.prepClose(err)
		return err
	}
	_, err = writeFull(tx.txTrp, payload)
	if err != nil {
		tx.prepClose(err)
	}
	return err
}

// WriteSubscribe writes an SUBSCRIBE packet over the transport.
func (tx *Tx) WriteSubscribe(varSub VariablesSubscribe) error {
	if tx.txTrp == nil {
		return errors.New("nil transport")
	}
	h := newHeader(PacketSubscribe, PacketFlagsPubrelSubUnsub, uint32(varSub.Size()))
	_, err := h.Encode(tx.txTrp)
	if err != nil {
		tx.prepClose(err)
		return err
	}
	_, err = encodeSubscribe(tx.txTrp, varSub)
	if err != nil {
		tx.prepClose(err)
	}
	return err
}

// WriteSuback writes an UNSUBACK packet over the transport.
func (tx *Tx) WriteSuback(varSub VariablesSuback) error {
	if tx.txTrp == nil {
		return errors.New("nil transport")
	}
	if err := varSub.Validate(); err != nil {
		return err
	}
	h := newHeader(PacketSuback, 0, uint32(varSub.Size()))
	_, err := h.Encode(tx.txTrp)
	if err != nil {
		tx.prepClose(err)
		return err
	}
	_, err = encodeSuback(tx.txTrp, varSub)
	if err != nil {
		tx.prepClose(err)
	}
	return err
}

// WriteUnsubscribe writes an UNSUBSCRIBE packet over the transport.
func (tx *Tx) WriteUnsubscribe(varUnsub VariablesUnsubscribe) error {
	if tx.txTrp == nil {
		return errors.New("nil transport")
	}
	h := newHeader(PacketUnsubscribe, PacketFlagsPubrelSubUnsub, uint32(varUnsub.Size()))
	_, err := h.Encode(tx.txTrp)
	if err != nil {
		tx.prepClose(err)
		return err
	}
	_, err = encodeUnsubscribe(tx.txTrp, varUnsub)
	if err != nil {
		tx.prepClose(err)
	}
	return err
}

// WriteOther writes PUBACK, PUBREC, PUBREL, PUBCOMP, UNSUBACK packets containing non-zero packet identfiers
// and DISCONNECT, PINGREQ, PINGRESP packets with no packet identifier. It automatically sets the RemainingLength field.
func (tx *Tx) WriteOther(h Header, packetIdentifier uint16) (err error) {
	if tx.txTrp == nil {
		return errors.New("nil transport")
	}
	hasPI := h.HasPacketIdentifier()
	if hasPI {
		h.RemainingLength = 2
		_, err = h.Encode(tx.txTrp)
		if err != nil {
			tx.prepClose(err)
			return err
		}
		_, err = encodeUint16(tx.txTrp, packetIdentifier)
	} else {
		h.RemainingLength = 0
		_, err = h.Encode(tx.txTrp)
	}
	if err != nil {
		tx.prepClose(err)
	}
	return err
}

// WriteSimple facilitates easy sending of the 2 octet DISCONNECT, PINGREQ, PINGRESP packets.
// If the packet is not one of these then an error is returned.
// It also returns an error with encoding step if there was one.
func (tx *Tx) WriteSimple(packetType PacketType) (err error) {
	if tx.txTrp == nil {
		return errors.New("nil transport")
	}
	isValid := packetType == PacketDisconnect || packetType == PacketPingreq || packetType == PacketPingresp
	if !isValid {
		return errors.New("expected DISCONNECT, PINGREQ or PINGRESP packet")
	}
	_, err = newHeader(packetType, 0, 0).Encode(tx.txTrp)
	if err != nil {
		return err
	}
	return err
}

func (tx *Tx) prepClose(err error) {
	if tx.OnTxError != nil {
		tx.OnTxError(err)
	}
	err = tx.txTrp.Close()
	if tx.OnTxError != nil {
		tx.OnTxError(err)
	}
}
