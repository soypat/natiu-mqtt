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

// ShallowCopy shallow copies rxtx and underlying transports and encoders/decoders. Does not copy callbacks over.
func (rxtx *RxTx) ShallowCopy() *RxTx {
	return &RxTx{
		Tx: *rxtx.Tx.ShallowCopy(),
		Rx: *rxtx.Rx.ShallowCopy(),
	}
}

// SetTransport sets the rxtx's reader and writer.
func (rxtx *RxTx) SetTransport(transport io.ReadWriteCloser) {
	rxtx.rxTrp = transport
	rxtx.txTrp = transport
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
	// OnRxError is called if an error is encountered during decoding of packet.
	// If it is set then it becomes the responsibility of the callback to close the transport.
	OnRxError func(*Rx, error)

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

// SetRxTransport sets the rx's reader.
func (rx *Rx) SetRxTransport(transport io.ReadCloser) {
	rx.rxTrp = transport
}

// Close closes the underlying transport.
func (rx *Rx) CloseRx() error { return rx.rxTrp.Close() }
func (rx *Rx) rxErrHandler(err error) {
	if rx.OnRxError != nil {
		rx.OnRxError(rx, err)
	} else {
		rx.CloseRx()
	}
}

// ReadNextPacket reads the next packet in the transport. If it fails after reading a
// non-zero amount of bytes it closes the transport and the underlying transport must be reset.
func (rx *Rx) ReadNextPacket() (int, error) {
	if rx.rxTrp == nil {
		return 0, errors.New("nil transport")
	}
	rx.LastReceivedHeader = Header{}
	hdr, n, err := DecodeHeader(rx.rxTrp)
	if err != nil {
		if n > 0 {
			rx.rxErrHandler(err)
		}
		return n, err
	}
	rx.LastReceivedHeader = hdr
	var (
		ngot             int
		packetIdentifier uint16
	)
	switch hdr.Type() {
	case PacketPublish:
		var vp VariablesPublish
		vp, ngot, err = rx.userDecoder.DecodePublish(rx.rxTrp, hdr.Flags().QoS())
		n += ngot
		if err != nil {
			break
		}
		payloadLen := int(hdr.RemainingLength) - ngot
		lr := io.LimitedReader{R: rx.rxTrp, N: int64(payloadLen)}
		if rx.OnPub != nil {
			err = rx.OnPub(rx, vp, &lr)
		} else {
			err = rx.exhaustReader(&lr)
		}

		if lr.N != 0 && err == nil {
			err = errors.New("expected OnPub to completely read payload")
			break
		}

	case PacketConnack:
		var vc VariablesConnack
		vc, ngot, err = rx.dec.DecodeConnack(rx.rxTrp)
		n += ngot
		if err != nil {
			break
		}
		if rx.OnConnack != nil {
			err = rx.OnConnack(rx, vc)
		}

	case PacketConnect:
		var vc VariablesConnect
		vc, ngot, err = rx.userDecoder.DecodeConnect(rx.rxTrp)
		n += ngot
		if err != nil {
			break
		}
		if rx.OnConnect != nil {
			err = rx.OnConnect(rx, &vc)
		}

	case PacketSuback:
		var vsbck VariablesSuback
		vsbck, ngot, err = rx.dec.DecodeSuback(rx.rxTrp, hdr.RemainingLength)
		n += ngot
		if err != nil {
			break
		}
		if rx.OnSuback != nil {
			err = rx.OnSuback(rx, vsbck)
		}

	case PacketSubscribe:
		var vsbck VariablesSubscribe
		vsbck, ngot, err = rx.userDecoder.DecodeSubscribe(rx.rxTrp, hdr.RemainingLength)
		n += ngot
		if err != nil {
			break
		}
		if rx.OnSub != nil {
			err = rx.OnSub(rx, vsbck)
		}

	case PacketUnsubscribe:
		var vunsub VariablesUnsubscribe
		vunsub, ngot, err = rx.userDecoder.DecodeUnsubscribe(rx.rxTrp, hdr.RemainingLength)
		n += ngot
		if err != nil {
			break
		}
		if rx.OnUnsub != nil {
			err = rx.OnUnsub(rx, vunsub)
		}

	case PacketPuback, PacketPubrec, PacketPubrel, PacketPubcomp, PacketUnsuback:
		// Only PI, no payload.
		packetIdentifier, ngot, err = decodeUint16(rx.rxTrp)
		n += ngot
		if err != nil {
			break
		}
		fallthrough
	case PacketDisconnect, PacketPingreq, PacketPingresp:
		// No payload or variable header.
		if rx.OnOther != nil {
			err = rx.OnOther(rx, packetIdentifier)
		}

	default:
		// Header Decode should return an error on incorrect packet type receive.
		// This could be tested via fuzzing.
		panic("unreachable")
	}

	if err != nil {
		rx.rxErrHandler(err)
	}
	return n, err
}

// ShallowCopy shallow copies rx and underlying transport and decoder. Does not copy callbacks over.
func (rx *Rx) ShallowCopy() *Rx {
	return &Rx{rxTrp: rx.rxTrp, userDecoder: rx.userDecoder}
}

func (rx *Rx) exhaustReader(r io.Reader) (err error) {
	if len(rx.ScratchBuf) == 0 {
		rx.ScratchBuf = make([]byte, 1024) // Lazy initialization when needed.
	}
	for err == nil {
		_, err = r.Read(rx.ScratchBuf[:])
	}
	if errors.Is(err, io.EOF) {
		return nil
	}
	return err
}

// Tx implements
type Tx struct {
	txTrp io.WriteCloser
	// OnTxError is called if an error is encountered during encoding. If it is set
	// then it becomes the responsibility of the callback to close Tx's transport.
	OnTxError      func(*Tx, error)
	OnSuccessfulTx func(*Tx)
}

// SetTxTransport sets the rxtx's reader and writer.
func (rxtx *Tx) SetTxTransport(transport io.WriteCloser) {
	rxtx.txTrp = transport
}

// WriteConnack writes a CONNECT packet over the transport.
func (tx *Tx) WriteConnect(varConn *VariablesConnect) error {
	if tx.txTrp == nil {
		return errors.New("nil transport")
	}
	h := newHeader(PacketConnect, 0, uint32(varConn.Size()))
	n, err := h.Encode(tx.txTrp)
	if err != nil {
		if n > 0 {
			tx.prepClose(err)
		}
		return err
	}
	_, err = encodeConnect(tx.txTrp, varConn)
	if err != nil {
		tx.prepClose(err)
	} else if tx.OnSuccessfulTx != nil {
		tx.OnSuccessfulTx(tx)
	}
	return err
}

// WriteConnack writes a CONNACK packet over the transport.
func (tx *Tx) WriteConnack(varConnack VariablesConnack) error {
	if tx.txTrp == nil {
		return errors.New("nil transport")
	}
	h := newHeader(PacketConnack, 0, uint32(varConnack.Size()))
	n, err := h.Encode(tx.txTrp)
	if err != nil {
		if n > 0 {
			tx.prepClose(err)
		}
		return err
	}
	_, err = encodeConnack(tx.txTrp, varConnack)
	if err != nil {
		tx.prepClose(err)
	} else if tx.OnSuccessfulTx != nil {
		tx.OnSuccessfulTx(tx)
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
	n, err := h.Encode(tx.txTrp)
	if err != nil {
		if n > 0 {
			tx.prepClose(err)
		}
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
	} else if tx.OnSuccessfulTx != nil {
		tx.OnSuccessfulTx(tx)
	}
	return err
}

// WriteSubscribe writes an SUBSCRIBE packet over the transport.
func (tx *Tx) WriteSubscribe(varSub VariablesSubscribe) error {
	if tx.txTrp == nil {
		return errors.New("nil transport")
	}
	h := newHeader(PacketSubscribe, PacketFlagsPubrelSubUnsub, uint32(varSub.Size()))
	n, err := h.Encode(tx.txTrp)
	if err != nil {
		if n > 0 {
			tx.prepClose(err)
		}
		return err
	}
	_, err = encodeSubscribe(tx.txTrp, varSub)
	if err != nil {
		tx.prepClose(err)
	} else if tx.OnSuccessfulTx != nil {
		tx.OnSuccessfulTx(tx)
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
	n, err := h.Encode(tx.txTrp)
	if err != nil {
		if n > 0 {
			tx.prepClose(err)
		}
		return err
	}
	_, err = encodeSuback(tx.txTrp, varSub)
	if err != nil {
		tx.prepClose(err)
	} else if tx.OnSuccessfulTx != nil {
		tx.OnSuccessfulTx(tx)
	}
	return err
}

// WriteUnsubscribe writes an UNSUBSCRIBE packet over the transport.
func (tx *Tx) WriteUnsubscribe(varUnsub VariablesUnsubscribe) error {
	if tx.txTrp == nil {
		return errors.New("nil transport")
	}
	h := newHeader(PacketUnsubscribe, PacketFlagsPubrelSubUnsub, uint32(varUnsub.Size()))
	n, err := h.Encode(tx.txTrp)
	if err != nil {
		if n > 0 {
			tx.prepClose(err)
		}
		return err
	}
	_, err = encodeUnsubscribe(tx.txTrp, varUnsub)
	if err != nil {
		tx.prepClose(err)
	} else if tx.OnSuccessfulTx != nil {
		tx.OnSuccessfulTx(tx)
	}
	return err
}

// WriteIdentified writes PUBACK, PUBREC, PUBREL, PUBCOMP, UNSUBACK packets containing non-zero packet identfiers
// It automatically sets the RemainingLength field to 2.
func (tx *Tx) WriteIdentified(packetType PacketType, packetIdentifier uint16) (err error) {
	if tx.txTrp == nil {
		return errors.New("nil transport")
	}
	if packetIdentifier == 0 {
		return errGotZeroPI
	}
	// This packet has special QoS1 flag.
	isPubrelSubUnsub := packetType == PacketPubrel
	if !(isPubrelSubUnsub || packetType == PacketPuback || packetType == PacketPubrec ||
		packetType == PacketPubcomp || packetType == PacketUnsuback) {
		return errors.New("expected a packet type from PUBACK|PUBREL|PUBCOMP|UNSUBACK")
	}
	n, err := newHeader(packetType, PacketFlags(b2u8(isPubrelSubUnsub)<<1), 2).Encode(tx.txTrp)
	if err != nil {
		if n > 0 {
			tx.prepClose(err)
		}
		return err
	}
	_, err = encodeUint16(tx.txTrp, packetIdentifier)
	if err != nil {
		tx.prepClose(err)
	} else if tx.OnSuccessfulTx != nil {
		tx.OnSuccessfulTx(tx)
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
		return errors.New("expected packet type from PINGREQ|PINGRESP|DISCONNECT")
	}
	n, err := newHeader(packetType, 0, 0).Encode(tx.txTrp)
	if err != nil && n > 0 {
		tx.prepClose(err)
	} else if tx.OnSuccessfulTx != nil {
		tx.OnSuccessfulTx(tx)
	}
	return err
}

// Close closes the underlying tranport and returns an error if any.
func (tx *Tx) CloseTx() error { return tx.txTrp.Close() }

func (tx *Tx) prepClose(err error) {
	if tx.OnTxError != nil {
		tx.OnTxError(tx, err)
	} else {
		tx.txTrp.Close()
	}
}

// ShallowCopy shallow copies rx and underlying transport and encoder. Does not copy callbacks over.
func (tx *Tx) ShallowCopy() *Tx {
	return &Tx{txTrp: tx.txTrp}
}
