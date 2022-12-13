package mqtt

import (
	"errors"
	"io"
	"net"
	"time"
)

// Client is a blocking MQTT v3.1.1 client implementation.
// The first field of the Client type will always be the RxTx non-pointer type.
type Client struct {
	rxtx RxTx
	// ID is the ClientID field in CONNECT packets.
	ID     string
	lastRx time.Time
}

func NewClient(decoder Decoder) *Client {
	return &Client{rxtx: RxTx{Rx: Rx{userDecoder: decoder}}}
}

// Connect sends a CONNECT packet over the transport. This is the first packet expected by a server.
// Connect returns the result of the interaction, with vconnack holding the server's return code.
// If the server
func (c *Client) Connect(vc *VariablesConnect) (vconnack VariablesConnack, err error) {
	if c.ID == "" {
		return VariablesConnack{}, errors.New("need to define a Client ID")
	}
	vc.ClientID = []byte(c.ID)
	err = c.rxtx.WriteConnect(vc)
	if err != nil {
		return VariablesConnack{}, err
	}
	previousCallback := c.rxtx.OnConnack
	c.rxtx.OnConnack = func(rt *Rx, vc VariablesConnack) error {
		if vc.ReturnCode != 0 {
			return errors.New(vc.ReturnCode.String())
		}
		c.lastRx = time.Now()
		vconnack = vc
		return nil
	}
	defer func() { c.rxtx.OnConnack = previousCallback }() // reset callback on exit.

	_, err = c.rxtx.ReadNextPacket()
	gotConnack := c.rxtx.LastReceivedHeader.Type() == PacketConnack
	if err == nil && !gotConnack {
		return VariablesConnack{}, errors.New("expected CONNACK response to CONNECT packet")
	}
	return vconnack, err
}

func (c *Client) Disconnect() error {
	if !c.lastRx.IsZero() {
		return errors.New("not connected")
	}
	err := c.rxtx.WriteSimple(PacketDisconnect)
	if err == nil || errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
		err = nil              //if EOF or network closed simply exit.
		c.lastRx = time.Time{} // Set back to zero value indicating no connection.
	}
	return err
}

// Subscribe sends a SUBSCRIBE packet over the transport.
func (c *Client) Subscribe(vsub VariablesSubscribe) (suback VariablesSuback, err error) {
	if err := vsub.Validate(); err != nil {
		return VariablesSuback{}, err
	}
	err = c.rxtx.WriteSubscribe(vsub)
	if err != nil {
		return VariablesSuback{}, err
	}
	previousCallback := c.rxtx.OnSuback
	c.rxtx.OnSuback = func(rt *Rx, vs VariablesSuback) error {
		c.lastRx = time.Now()
		suback = vs
		// if previousCallback !
		return nil
	}
	defer func() { c.rxtx.OnSuback = previousCallback }() // reset callback on exit.

	_, err = c.rxtx.ReadNextPacket()
	if err == nil && c.rxtx.LastReceivedHeader.Type() != PacketSuback {
		return VariablesSuback{}, errors.New("expected SUBACK response to SUBSCRIBE packet")
	}
	return suback, err
}

func (c *Client) PublishPayload(hdr Header, vp VariablesPublish, payload []byte) error {
	if err := vp.Validate(); err != nil {
		return err
	}
	if hdr.Flags().QoS() != QoS0 {
		return errors.New("only support QoS0")
	}
	return c.rxtx.WritePublishPayload(hdr, vp, payload)
}

// SetTransport sets the underlying transport. This allows users to re-open
// failed/closed connections on [RxTx] side and resuming communication with server.
func (c *Client) SetTransport(transport io.ReadWriteCloser) {
	c.rxtx.SetTransport(transport)
}

// Ping writes a PINGREQ packet over the network and blocks until a packet is received.
// If an error is encountered during decoding or if the received packet is not a PINGRESP then an error is returned
func (c *Client) Ping() error {
	err := c.rxtx.WriteSimple(PacketPingreq)
	if err != nil {
		return err
	}
	_, err = c.rxtx.ReadNextPacket()
	if err != nil {
		return err
	}
	if c.rxtx.LastReceivedHeader.Type() != PacketPingresp {
		return errors.New("expected PINGRESP response for PINGREQ")
	}
	return nil
}

// RxTx returns a new RxTx that wraps the transport layer.
// The returned RxTx uses the client's Decoder as is.
func (c *Client) RxTx() *RxTx {
	return c.rxtx.ShallowCopy()
}

// UnsafeRxTxPointer do not use unless you know what you are doing.
// Unsafe because it creates shared state regarding the LastReceivedHeader
// and callbacks. If these are overwritten on callers side they also overwrite the Client's callbacks.
func (c *Client) UnsafeRxTxPointer() *RxTx {
	return &c.rxtx
}
