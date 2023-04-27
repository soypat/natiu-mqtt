package mqtt

import (
	"errors"
	"io"
	"sync"
	"time"
)

type clientState struct {
	mu             sync.Mutex
	lastRx         time.Time
	lastTx         time.Time
	connectedAt    time.Time
	pendingSubs    VariablesSubscribe
	activeSubs     []string
	pendingPingreq time.Time
	// closeErr stores the reason for disconnection.
	closeErr error
}

// onConnect is meant to be called on opening a new connection to delete
// previous connection state. Not guarded by mutex.
func (cs *clientState) onConnect(t time.Time) {
	cs.closeErr = nil
	if cs.activeSubs == nil {
		cs.activeSubs = make([]string, 2)
	}
	cs.activeSubs = cs.activeSubs[:0]
	cs.lastRx = t
	cs.connectedAt = t
	cs.pendingPingreq = time.Time{}
	cs.pendingSubs = VariablesSubscribe{}
}

// onConnect is meant to be called on opening a new connection to delete
// previous connection state.
func (cs *clientState) OnDisconnect(err error) {
	if err == nil {
		panic("onDisconnect expects non-nil error")
	}
	cs.mu.Lock()
	cs.closeErr = err
	cs.connectedAt = time.Time{}
	cs.mu.Unlock()
}

// callbacks returns the Rx and Tx callbacks necessary for a clientState to function automatically.
// The onPub callback
func (cs *clientState) callbacks(onPub func(rx *Rx, varPub VariablesPublish, r io.Reader) error) (RxCallbacks, TxCallbacks) {
	closeConn := func(err error) {
		cs.mu.Lock()
		defer cs.mu.Unlock()
		cs.connectedAt = time.Time{}
		cs.closeErr = err
	}
	return RxCallbacks{
			OnConnack: func(r *Rx, vc VariablesConnack) error {
				connTime := time.Now()
				cs.mu.Lock()
				defer cs.mu.Unlock()
				cs.lastRx = connTime
				if cs.closeErr == nil {
					return errors.New("connack received while connected")
				}
				if vc.ReturnCode != 0 {
					return errors.New(vc.ReturnCode.String())
				}
				cs.onConnect(connTime)
				return nil
			},
			OnPub: onPub,
			OnSuback: func(r *Rx, vs VariablesSuback) error {
				rxTime := time.Now()
				cs.mu.Lock()
				defer cs.mu.Unlock()
				cs.lastRx = rxTime
				if len(vs.ReturnCodes) != len(cs.pendingSubs.TopicFilters) {
					return errors.New("got mismatched number of return codes compared to pending client subscriptions")
				}
				for i, qos := range vs.ReturnCodes {
					if qos != QoSSubfail {
						if qos != cs.pendingSubs.TopicFilters[i].QoS {
							return errors.New("QoS does not match requested QoS for topic")
						}
						cs.activeSubs = append(cs.activeSubs, string(cs.pendingSubs.TopicFilters[i].TopicFilter))
					}
				}
				cs.pendingSubs.TopicFilters = cs.pendingSubs.TopicFilters[:0]
				return nil
			},
			OnOther: func(rx *Rx, packetIdentifier uint16) (err error) {
				tp := rx.LastReceivedHeader.Type()
				rxTime := time.Now()
				cs.mu.Lock()
				defer cs.mu.Unlock()
				cs.lastRx = rxTime
				switch tp {
				case PacketDisconnect:
					cs.connectedAt = time.Time{}
					err = errors.New("received graceful disconnect request")
				case PacketPingreq:
					cs.pendingPingreq = rxTime
				default:
					println("unexpected packet type: ", tp.String())
				}
				if err != nil {
					cs.closeErr = err
				}
				return err
			},
			OnRxError: func(r *Rx, err error) {
				closeConn(err)
			},
			// OnOther: ,
		}, TxCallbacks{
			OnTxError: func(tx *Tx, err error) {
				closeConn(err)
			},
			OnSuccessfulTx: func(tx *Tx) {
				cs.mu.Lock()
				defer cs.mu.Unlock()
				cs.lastTx = time.Now()
			},
		}
}

// IsConnected returns true if the client is currently connected.
func (cs *clientState) IsConnected() bool {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	if cs.connectedAt.IsZero() == (cs.closeErr != nil) {
		panic("assertion failed: bug in natiu-mqtt clientState implementation")
	}
	return cs.closeErr == nil
}

// Err returns the error that caused the MQTT connection to finish.
// Returns nil if currently connected.
func (cs *clientState) Err() error {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	if cs.connectedAt.IsZero() == (cs.closeErr != nil) {
		panic("assertion failed: bug in natiu-mqtt clientState implementation")
	}
	return cs.closeErr
}

// PendingResponse returns true if the client is waiting on the server for a response.
func (cs *clientState) PendingResponse() bool {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	return cs.closeErr == nil && (len(cs.pendingSubs.TopicFilters) > 0 || !cs.pendingPingreq.IsZero())
}

func (cs *clientState) AwaitingPingresp() bool {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	return !cs.pendingPingreq.IsZero()
}

func (cs *clientState) AwaitingSuback() bool {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	return len(cs.pendingSubs.TopicFilters) > 0
}

func (cs *clientState) PingTime() time.Time {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	return cs.pendingPingreq
}

func (cs *clientState) PendingSublen() int {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	return len(cs.pendingSubs.TopicFilters)
}

func (cs *clientState) ConnectedAt() time.Time {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	return cs.connectedAt
}
