package mqtt

import (
	"errors"
	"io"
	"net"
	"sync"
)

var errDisconnected = errors.New("disconnected")

// Client is a asynchronous MQTT v3.1.1 client implementation.
// The first field of the Client type will always be the RxTx non-pointer type.
type Client struct {
	cs     clientState
	rx     Rx
	txlock sync.Mutex
	tx     Tx
}

func NewClient(decoder Decoder, onPub func(pubHead Header, varPub VariablesPublish, r io.Reader) error) *Client {
	c := &Client{cs: clientState{closeErr: errors.New("yet to connect")}}
	c.rx.RxCallbacks, c.tx.TxCallbacks = c.cs.callbacks(func(rx *Rx, varPub VariablesPublish, r io.Reader) error {
		return onPub(rx.LastReceivedHeader, varPub, r)
	})
	c.rx.userDecoder = decoder
	return c
}

// Connect sends a CONNECT packet over the transport and does not wait for a
// CONNACK response. Client is not guaranteed to be connected after a call to this function.
func (c *Client) StartConnect(rwc io.ReadWriteCloser, vc *VariablesConnect) error {
	if c.cs.IsConnected() {
		return errors.New("already connected; disconnect before connecting")
	}
	c.SetTransport(rwc)
	c.txlock.Lock()
	defer c.txlock.Unlock()
	return c.tx.WriteConnect(vc)
}

// IsConnected returns true if there still has been no disconnect event or an
// unrecoverable error encountered during decoding.
// A Connected client may send and receive MQTT messages.
func (c *Client) IsConnected() bool { return c.cs.IsConnected() }

// Disconnect performs a MQTT disconnect and resets the connection. Future
// calls to Err will return the argument userErr.
func (c *Client) Disconnect(userErr error) error {
	if userErr == nil {
		panic("nil error argument to Disconnect")
	}
	if c.IsConnected() {
		return errDisconnected
	}
	c.cs.OnDisconnect(userErr)
	c.txlock.Lock()
	defer c.txlock.Unlock()
	err := c.tx.WriteSimple(PacketDisconnect)
	if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
		err = nil //if EOF or network closed simply exit.
	}
	return err
}

// StartSubscribe begins subscription to argument topics.
func (c *Client) StartSubscribe(vsub VariablesSubscribe) error {
	if !c.IsConnected() {
		return errDisconnected
	}
	if c.AwaitingSuback() {
		return errors.New("tried to subscribe while still awaiting suback")
	}
	if err := vsub.Validate(); err != nil {
		return err
	}
	return c.tx.WriteSubscribe(vsub)
}

// SubscribedTopics returns list of topics the client succesfully subscribed to.
func (c *Client) SubscribedTopics() []string {
	c.cs.mu.Lock()
	defer c.cs.mu.Unlock()
	return c.cs.activeSubs
}

func (c *Client) PublishPayload(hdr Header, vp VariablesPublish, payload []byte) error {
	if !c.IsConnected() {
		return errDisconnected
	}
	if err := vp.Validate(); err != nil {
		return err
	}
	if hdr.Flags().QoS() != QoS0 {
		return errors.New("only support QoS0")
	}
	return c.tx.WritePublishPayload(hdr, vp, payload)
}

// Err returns error indicating the cause of client disconnection.
func (c *Client) Err() error {
	return c.cs.Err()
}

// SetTransport sets the underlying transport. This allows users to re-open
// failed/closed connections on [RxTx] side and resuming communication with server.
func (c *Client) SetTransport(transport io.ReadWriteCloser) {
	c.tx.SetTxTransport(transport)
	c.rx.SetRxTransport(transport)
}

// Ping writes a PINGREQ packet over the network and does not block.
func (c *Client) StartPing() error {
	if !c.IsConnected() {
		return errDisconnected
	}
	return c.tx.WriteSimple(PacketPingreq)
}

// AwaitingPingresp checks if a ping sent over the wire had no response received back.
func (c *Client) AwaitingPingresp() bool { return c.cs.AwaitingPingresp() }

// AwaitingSuback checks if a subscribe request sent over the wire had no suback received back.
func (c *Client) AwaitingSuback() bool { return c.cs.AwaitingSuback() }
