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
	// Transport
	trp io.ReadWriteCloser
	// User defined decoder for allocating packets.
	userDec Decoder
	// Default decoder for non allocating packets.
	dec DecoderLowmem
}

// NewRxTx creates a new RxTx. Before use user must configure OnX fields by setting a function
// to perform an action each time a packet is received.
func NewRxTx(transport io.ReadWriteCloser, decoder Decoder) (*RxTx, error) {
	if transport == nil || decoder == nil {
		return nil, errors.New("got nil transport io.ReadWriteCloser or nil Decoder")
	}
	cc := &RxTx{
		trp:     transport,
		userDec: decoder,
		// No memory needed for DecoderLowmem for this use.
		dec: DecoderLowmem{},
		// Define no-op functions to prevent nil function calls. These ignore packets as they arrive.
		OnConnack: func(*RxTx, VariablesConnack) error { return nil },
		OnPub:     func(*RxTx, VariablesPublish, io.Reader) error { return nil },
		OnConnect: func(*RxTx, *VariablesConnect) error { return nil },
		OnSuback:  func(*RxTx, VariablesSuback) error { return nil },
		OnSub:     func(*RxTx, VariablesSubscribe) error { return nil },
		OnUnsub:   func(*RxTx, VariablesUnsubscribe) error { return nil },
		OnOther:   func(*RxTx, uint16) error { return nil },
	}
	return cc, nil
}

// Close closes the underlying transport.
func (rxtx *RxTx) Close() error { return rxtx.trp.Close() }

func (rxtx *RxTx) ReadNextPacket() (int, error) {
	hdr, n, err := DecodeHeader(rxtx.trp)
	if err != nil {
		rxtx.trp.Close()
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
		vp, ngot, err = rxtx.userDec.DecodePublish(rxtx.trp, hdr.Flags().QoS())
		n += ngot
		if err != nil {
			break
		}
		payloadLen := int(hdr.RemainingLength) - ngot
		lr := io.LimitedReader{R: rxtx.trp, N: int64(payloadLen)}
		err = rxtx.OnPub(rxtx, vp, &lr)
		if lr.N != 0 && err == nil {
			err = errors.New("OnPub did not completely read payload")
			break
		}

	case PacketConnack:
		var vc VariablesConnack
		vc, ngot, err = rxtx.dec.DecodeConnack(rxtx.trp)
		n += ngot
		if err != nil {
			break
		}
		err = rxtx.OnConnack(rxtx, vc)

	case PacketConnect:
		var vc VariablesConnect
		vc, ngot, err = rxtx.userDec.DecodeConnect(rxtx.trp)
		n += ngot
		if err != nil {
			break
		}
		err = rxtx.OnConnect(rxtx, &vc)

	case PacketSuback:
		var vsbck VariablesSuback
		vsbck, ngot, err = rxtx.dec.DecodeSuback(rxtx.trp, hdr.RemainingLength)
		n += ngot
		if err != nil {
			break
		}
		err = rxtx.OnSuback(rxtx, vsbck)

	case PacketSubscribe:
		var vsbck VariablesSubscribe
		vsbck, ngot, err = rxtx.userDec.DecodeSubscribe(rxtx.trp, hdr.RemainingLength)
		n += ngot
		if err != nil {
			break
		}
		err = rxtx.OnSub(rxtx, vsbck)

	case PacketUnsubscribe:
		var vunsub VariablesUnsubscribe
		vunsub, ngot, err = rxtx.userDec.DecodeUnsubscribe(rxtx.trp, hdr.RemainingLength)
		n += ngot
		if err != nil {
			break
		}
		err = rxtx.OnUnsub(rxtx, vunsub)

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
		err = rxtx.OnOther(rxtx, packetIdentifier)
	default:
		panic("unreachable")
	}

	if err != nil {
		rxtx.trp.Close()
	}
	return n, err
}

// WriteConnack encodes a CONNECT packet over the wire.
func (rxtx *RxTx) WriteConnect(h Header, varConn *VariablesConnect) error {
	_, err := h.Encode(rxtx.trp)
	if err != nil {
		return err
	}
	_, err = encodeConnect(rxtx.trp, varConn)
	return err
}

// WriteConnack encodes a CONNACK packet over the wire.
func (rxtx *RxTx) WriteConnack(h Header, varConnack VariablesConnack) error {
	_, err := h.Encode(rxtx.trp)
	if err != nil {
		return err
	}
	_, err = encodeConnack(rxtx.trp, varConnack)
	return err
}

func (rxtx *RxTx) WritePublish(h Header, varPub VariablesPublish) error {
	_, err := h.Encode(rxtx.trp)
	if err != nil {
		return err
	}
	_, err = encodePublish(rxtx.trp, varPub)
	return err
}

func (rxtx *RxTx) WriteOther(h Header, packetIdentifier uint16) error {
	_, err := h.Encode(rxtx.trp)
	if err != nil {
		return err
	}
	return nil
}
