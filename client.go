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

// Client is a asynchronous MQTT v3.1.1 client implementation which is
// safe for concurrent use.
type Client struct {
	cs clientState

	rxlock sync.Mutex
	rx     Rx

	txlock sync.Mutex
	tx     Tx
}

// ClientConfig is used to configure a new Client.
type ClientConfig struct {
	// If a Decoder is not set one will automatically be picked.
	Decoder Decoder
	// OnPub is executed on every PUBLISH message received. Do not call
	// HandleNext or other client methods from within this function.
	OnPub func(pubHead Header, varPub VariablesPublish, r io.Reader) error
	// TODO: add a backoff algorithm callback here so clients can roll their own.
}

// NewClient creates a new MQTT client with the configuration parameters provided.
// If no Decoder is provided a DecoderNoAlloc will be used.
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
	_, err := c.readNextWrapped()
	if err != nil && c.IsConnected() {
		// This probably never executes since rxOnError should disconnect client, here for even more guarantees.
		c.cs.OnDisconnect(err)
		c.txlock.Lock()
		c.tx.WriteSimple(PacketDisconnect)
		c.txlock.Unlock()
	}
	return err
}

// readNextWrapped is a separate function so mutex locks Rx for minimum amount of time.
func (c *Client) readNextWrapped() (int, error) {
	c.rxlock.Lock()
	defer c.rxlock.Unlock()
	if !c.IsConnected() && c.cs.lastTx.IsZero() {
		// Client disconnected and not expecting to receive packets back.
		return 0, errDisconnected
	}
	return c.rx.ReadNextPacket()
}

// StartConnect sends a CONNECT packet over the transport and does not wait for a
// CONNACK response. Client is not guaranteed to be connected after a call to this function.
func (c *Client) StartConnect(rwc io.ReadWriteCloser, vc *VariablesConnect) error {
	c.rxlock.Lock()
	defer c.rxlock.Unlock()
	c.txlock.Lock()
	defer c.txlock.Unlock()
	c.tx.SetTxTransport(rwc)
	c.rx.SetRxTransport(rwc)
	if c.cs.IsConnected() {
		return errors.New("already connected; disconnect before connecting")
	}
	return c.tx.WriteConnect(vc)
}

// Connect sends a CONNECT packet over the transport and waits for a
// CONNACK response from the server. The client is connected if the returned error is nil.
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
	if c.IsConnected() {
		return nil
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
	c.txlock.Lock()
	defer c.txlock.Unlock()
	if !c.IsConnected() {
		return errDisconnected
	}
	c.cs.OnDisconnect(userErr)
	err := c.tx.WriteSimple(PacketDisconnect)
	if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
		err = nil //if EOF or network closed simply exit.
	}
	c.rxlock.Lock()
	defer c.rxlock.Unlock()
	c.rx.rxTrp.Close()
	c.tx.txTrp.Close()
	return err
}

// StartSubscribe begins subscription to argument topics.
func (c *Client) StartSubscribe(vsub VariablesSubscribe) error {
	if err := vsub.Validate(); err != nil {
		return err
	}
	c.txlock.Lock()
	defer c.txlock.Unlock()
	if !c.IsConnected() {
		return errDisconnected
	}
	if c.AwaitingSuback() {
		// TODO(soypat): Allow multiple subscriptions to be queued.
		return errors.New("tried to subscribe while still awaiting suback")
	}
	c.cs.pendingSubs = vsub.Copy()
	return c.tx.WriteSubscribe(vsub)
}

// Subscribe writes a SUBSCRIBE packet over the network and waits for the server
// to respond with a SUBACK packet or until the context ends.
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

// SubscribedTopics returns list of topics the client successfully subscribed to.
// Returns a copy of a slice so is safe for concurrent use.
func (c *Client) SubscribedTopics() []string {
	c.cs.mu.Lock()
	defer c.cs.mu.Unlock()
	return append([]string{}, c.cs.activeSubs...)
}

// PublishPayload sends a PUBLISH packet over the network on the topic defined by
// varPub.
func (c *Client) PublishPayload(flags PacketFlags, varPub VariablesPublish, payload []byte) error {
	if err := varPub.Validate(); err != nil {
		return err
	}
	qos := flags.QoS()
	if qos != QoS0 {
		return errors.New("only supports QoS0")
	}
	c.txlock.Lock()
	defer c.txlock.Unlock()
	if !c.IsConnected() {
		return errDisconnected
	}
	return c.tx.WritePublishPayload(newHeader(PacketPublish, flags, uint32(varPub.Size(qos)+len(payload))), varPub, payload)
}

// Err returns error indicating the cause of client disconnection.
func (c *Client) Err() error {
	return c.cs.Err()
}

// StartPing writes a PINGREQ packet over the network without blocking waiting for response.
func (c *Client) StartPing() error {
	c.txlock.Lock()
	defer c.txlock.Unlock()
	if !c.IsConnected() {
		return errDisconnected
	}
	err := c.tx.WriteSimple(PacketPingreq)
	if err == nil {
		c.cs.PingSent() // Flag the fact that a ping has been sent successfully.
	}
	return err
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
	pingTime := c.cs.LastPingTime()
	if pingTime.IsZero() {
		return nil // Ping completed.
	}
	backoff := newBackoff()
	for pingTime == c.cs.LastPingTime() && ctx.Err() == nil {
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

// ConnectedAt returns the time the client managed to successfully connect. If
// client is disconnected ConnectedAt returns the zero-value for time.Time.
func (c *Client) ConnectedAt() time.Time { return c.cs.ConnectedAt() }

// AwaitingSuback checks if a subscribe request sent over the wire had no suback received back.
// Returns false if client is disconnected.
func (c *Client) AwaitingSuback() bool { return c.cs.AwaitingSuback() }

// LastRx returns the time the last packet was received at.
// If Client is disconnected LastRx returns the zero value of time.Time.
func (c *Client) LastRx() time.Time { return c.cs.LastRx() }

// LastTx returns the time the last successful packet transmission finished at.
// A "successful" transmission does not necessarily mean the packet was received on the other end.
// If Client is disconnected LastTx returns the zero value of time.Time.
func (c *Client) LastTx() time.Time { return c.cs.LastTx() }

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
