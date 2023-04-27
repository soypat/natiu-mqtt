package mqtt

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"time"
)

var (
	errDisconnected = errors.New("natiu-mqtt: disconnected")
)

// Client is a asynchronous MQTT v3.1.1 client implementation.
// The first field of the Client type will always be the RxTx non-pointer type.
type Client struct {
	cs     clientState
	rx     Rx
	txlock sync.Mutex
	rxlock sync.Mutex
	tx     Tx
}

type ClientConfig struct {
	// If a Decoder is not set one will automatically be picked.
	Decoder Decoder
	// OnPub is executed on every PUBLISH message received. Do not call
	// HandleNext or other client methods from within this function.
	OnPub func(pubHead Header, varPub VariablesPublish, r io.Reader) error
}

func NewClient(cfg ClientConfig) *Client {
	var onPub func(rx *Rx, varPub VariablesPublish, r io.Reader) error
	if cfg.OnPub != nil {
		onPub = func(rx *Rx, varPub VariablesPublish, r io.Reader) error {
			return cfg.OnPub(rx.LastReceivedHeader, varPub, r)
		}
	}
	if cfg.Decoder == nil {
		cfg.Decoder = DecoderNoAlloc{UserBuffer: make([]byte, 4*1024)}
	}
	c := &Client{cs: clientState{closeErr: errors.New("yet to connect")}}
	c.rx.RxCallbacks, c.tx.TxCallbacks = c.cs.callbacks(onPub)
	c.rx.userDecoder = cfg.Decoder
	return c
}

// HandleNext reads from the wire and decodes MQTT packets.
// If bytes are read and the decoder fails to read a packet the whole
// client fails and disconnects.
// HandleNext only returns an error in the case where the OnPub callback passed
// in the ClientConfig returns an error or if a packet is malformed.
// If HandleNext returns an error the client will be in a disconnected state.
func (c *Client) HandleNext() error {
	if !c.IsConnected() {
		return errDisconnected
	}
	c.rxlock.Lock()
	defer c.rxlock.Unlock()
	n, err := c.rx.ReadNextPacket()
	if err != nil && n != 0 {
		if c.IsConnected() {
			c.cs.OnDisconnect(err)
			c.txlock.Lock()
			defer c.txlock.Unlock()
			c.tx.WriteSimple(PacketDisconnect)
		}
		return err
	}
	return err
}

// StartConnect sends a CONNECT packet over the transport and does not wait for a
// CONNACK response. Client is not guaranteed to be connected after a call to this function.
func (c *Client) StartConnect(rwc io.ReadWriteCloser, vc *VariablesConnect) error {
	if c.cs.IsConnected() {
		return errors.New("already connected; disconnect before connecting")
	}
	c.setTransport(rwc)
	c.txlock.Lock()
	defer c.txlock.Unlock()
	return c.tx.WriteConnect(vc)
}

// Connect sends a CONNECT packet over the transport and waits for a
// CONNACK response.
func (c *Client) Connect(ctx context.Context, rwc io.ReadWriteCloser, vc *VariablesConnect) error {
	err := c.StartConnect(rwc, vc)
	if err != nil {
		return err
	}
	backoff := newBackoff()
	for !c.IsConnected() && ctx.Err() == nil {
		backoff.Miss()
		err := c.HandleNext()
		if err != nil {
			return err
		}
	}
	return ctx.Err()
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
	c.rx.rxTrp.Close()
	c.tx.txTrp.Close()
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

// Ping writes a ping packet over the network and blocks until it receives the ping
// response back. It uses an exponential backoff algorithm to time checks on the
// status of the ping.
func (c *Client) Subscribe(ctx context.Context, vsub VariablesSubscribe) error {
	session := c.ConnectedAt()
	err := c.StartSubscribe(vsub)
	if err != nil {
		return err
	}
	backoff := newBackoff()
	for c.cs.PendingSublen() != 0 && ctx.Err() == nil {
		if c.ConnectedAt() != session {
			// Prevent waiting on subscribes from previous connection or during disconnection.
			return errDisconnected
		}
		backoff.Miss()
		c.HandleNext()
	}
	return ctx.Err()
}

// SubscribedTopics returns list of topics the client succesfully subscribed to.
func (c *Client) SubscribedTopics() []string {
	c.cs.mu.Lock()
	defer c.cs.mu.Unlock()
	return c.cs.activeSubs
}

// PublishPayload sends a PUBLISH packet over the network on the topic defined by
// varPub.
func (c *Client) PublishPayload(flags PacketFlags, varPub VariablesPublish, payload []byte) error {
	if !c.IsConnected() {
		return errDisconnected
	}
	if err := varPub.Validate(); err != nil {
		return err
	}
	qos := flags.QoS()
	if qos != QoS0 {
		return errors.New("only supports QoS0")
	}
	c.txlock.Lock()
	defer c.txlock.Unlock()
	return c.tx.WritePublishPayload(newHeader(PacketPublish, flags, uint32(varPub.Size(qos)+len(payload))), varPub, payload)
}

// Err returns error indicating the cause of client disconnection.
func (c *Client) Err() error {
	return c.cs.Err()
}

// setTransport sets the underlying transport. This allows users to re-open
// failed/closed connections on [RxTx] side and resuming communication with server.
func (c *Client) setTransport(transport io.ReadWriteCloser) {
	c.tx.SetTxTransport(transport)
	c.rx.SetRxTransport(transport)
}

// StartPing writes a PINGREQ packet over the network without blocking waiting for response.
func (c *Client) StartPing() error {
	if !c.IsConnected() {
		return errDisconnected
	}
	return c.tx.WriteSimple(PacketPingreq)
}

// Ping writes a ping packet over the network and blocks until it receives the ping
// response back. It uses an exponential backoff algorithm to time checks on the
// status of the ping.
func (c *Client) Ping(ctx context.Context) error {
	session := c.ConnectedAt()
	err := c.StartPing()
	if err != nil {
		return err
	}
	pingTime := c.cs.PingTime()
	if pingTime.IsZero() {
		return nil // Ping completed.
	}
	backoff := newBackoff()
	for pingTime == c.cs.PingTime() && ctx.Err() == nil {
		if c.ConnectedAt() != session {
			// Prevent waiting on subscribes from previous connection or during disconnection.
			return errDisconnected
		}
		backoff.Miss()
		c.HandleNext()
	}
	return ctx.Err()
}

// AwaitingPingresp checks if a ping sent over the wire had no response received back.
func (c *Client) AwaitingPingresp() bool { return c.cs.AwaitingPingresp() }

func (c *Client) ConnectedAt() time.Time { return c.cs.ConnectedAt() }

// AwaitingSuback checks if a subscribe request sent over the wire had no suback received back.
func (c *Client) AwaitingSuback() bool { return c.cs.AwaitingSuback() }

func newBackoff() exponentialBackoff {
	return exponentialBackoff{
		MaxWait: 500 * time.Millisecond,
	}
}

// exponentialBackoff implements a [Exponential Backoff]
// delay algorithm to prevent saturation network or processor
// with failing tasks. An exponentialBackoff with a non-zero MaxWait is ready for use.
//
// [Exponential Backoff]: https://en.wikipedia.org/wiki/Exponential_backoff
type exponentialBackoff struct {
	// Wait defines the amount of time that Miss will wait on next call.
	Wait time.Duration
	// Maximum allowable value for Wait.
	MaxWait time.Duration
	// StartWait is the value that Wait takes after a call to Hit.
	StartWait time.Duration
	// ExpMinusOne is the shift performed on Wait minus one, so the zero value performs a shift of 1.
	ExpMinusOne uint32
}

// Hit sets eb.Wait to the StartWait value.
func (eb *exponentialBackoff) Hit() {
	if eb.MaxWait == 0 {
		panic("MaxWait cannot be zero")
	}
	eb.Wait = eb.StartWait
}

// Miss sleeps for eb.Wait and increases eb.Wait exponentially.
func (eb *exponentialBackoff) Miss() {
	const k = 1
	wait := eb.Wait
	maxWait := eb.MaxWait
	exp := eb.ExpMinusOne + 1
	if maxWait == 0 {
		panic("MaxWait cannot be zero")
	}
	time.Sleep(wait)
	wait |= time.Duration(k)
	wait <<= exp
	if wait > maxWait {
		wait = maxWait
	}
	eb.Wait = wait
}
