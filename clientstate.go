package mqtt

import (
	"errors"
	"io"
	"sync"
	"time"
)

type clientState struct {
	mu          sync.Mutex
	lastRx      int64
	lastTx      int64
	connectedAt int64
	activeSubs  []string
	// field flag indicates we received a ping request from server and need to reply.
	pendingPingreq int64
	// field flags we are waiting on a ping response packet from server.
	pendingPingresp int64
	// closeErr stores the reason for disconnection.
	closeErr    error
	pendingSubs VariablesSubscribe
	_nanotime   func() int64
	// orchestrator handles QoS1/QoS2 state. Nil for a pure QoS0 client, in which
	// case all QoS>0 routing and retransmission pumping is inert.
	orchestrator QoSOrchestrator
}

func (cs *clientState) nanotime() int64 {
	if cs._nanotime != nil {
		return cs._nanotime()
	}
	return time.Now().UnixNano()
}

// onConnect is meant to be called on opening a new connection to delete
// previous connection state. Not guarded by mutex.
func (cs *clientState) onConnect(t int64) {
	cs.closeErr = nil
	if cs.activeSubs == nil {
		cs.activeSubs = make([]string, 2)
	}
	cs.activeSubs = cs.activeSubs[:0]
	cs.lastRx = t
	cs.connectedAt = t
	cs.pendingSubs = VariablesSubscribe{}
}

// onConnect is meant to be called on opening a new connection to delete
// previous connection state.
func (cs *clientState) OnDisconnect(err error) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.onDisconnect(err)
}

//go:inline
func (cs *clientState) onDisconnect(err error) {
	if err == nil {
		panic("onDisconnect expects non-nil error")
	}
	cs.closeErr = err
	cs.connectedAt = 0
	cs.lastRx = 0
	cs.lastTx = 0
	cs.pendingPingreq = 0
	cs.pendingPingresp = 0
	cs.pendingSubs = VariablesSubscribe{}
}

// callbacks returns the Rx and Tx callbacks necessary for a clientState to function automatically.
// The onPub callback
func (cs *clientState) callbacks(onPub func(rx *Rx, varPub VariablesPublish, r io.Reader) error) (RxCallbacks, TxCallbacks) {
	return RxCallbacks{
			OnConnack: func(r *Rx, vc VariablesConnack) error {
				connTime := cs.nanotime()
				cs.mu.Lock()
				defer cs.mu.Unlock()
				cs.lastRx = connTime
				if cs.closeErr == nil {
					return errors.New("connack received while connected")
				}
				if vc.ReturnCode != 0 {
					return vc.ReturnCode
				}
				cs.onConnect(connTime)
				return nil
			},
			OnPub: func(rx *Rx, vp VariablesPublish, r io.Reader) error {
				// Inbound QoS>0 PUBLISH must be reported to the orchestrator so it
				// can advance the inbound state machine (enqueue PUBACK/PUBREC).
				if rx.LastReceivedHeader.Flags().QoS() != QoS0 {
					cs.mu.Lock()
					orch := cs.orchestrator
					cs.mu.Unlock()
					if orch != nil {
						if err := orch.OnControlPacket(rx.LastReceivedHeader, vp.PacketIdentifier); err != nil {
							return err
						}
					}
				}
				if onPub != nil {
					return onPub(rx, vp, r)
				}
				return nil
			},
			OnSuback: func(r *Rx, vs VariablesSuback) error {
				rxTime := cs.nanotime()
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
				rxTime := cs.nanotime()
				cs.mu.Lock()
				cs.lastRx = rxTime
				var orch QoSOrchestrator
				switch tp {
				case PacketDisconnect:
					err = errDisconnected
				case PacketPingreq:
					cs.pendingPingreq = rxTime
				case PacketPingresp:
					cs.pendingPingresp = 0 // got the response, we can unflag.
				case PacketPuback, PacketPubrec, PacketPubrel, PacketPubcomp:
					// QoS1/QoS2 control packets are routed to the orchestrator.
					orch = cs.orchestrator
				default:
					println("unexpected packet type: ", tp.String())
				}
				if err != nil {
					cs.onDisconnect(err)
				}
				cs.mu.Unlock()
				// Call orchestrator outside cs.mu to avoid re-entrant deadlock.
				if orch != nil {
					err = orch.OnControlPacket(rx.LastReceivedHeader, packetIdentifier)
				}
				return err
			},
			OnRxError: func(r *Rx, err error) {
				cs.onDisconnect(err)
			},
		}, TxCallbacks{
			OnTxError: func(tx *Tx, err error) {
				cs.onDisconnect(err)
			},
			OnSuccessfulTx: func(tx *Tx) {
				cs.mu.Lock()
				defer cs.mu.Unlock()
				cs.lastTx = cs.nanotime()
			},
		}
}

// IsConnected returns true if the client is currently connected.
func (cs *clientState) IsConnected() bool {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	if (cs.connectedAt == 0) != (cs.closeErr != nil) {
		panic("assertion failed: bug in natiu-mqtt clientState implementation")
	}
	return cs.closeErr == nil
}

// Err returns the error that caused the MQTT connection to finish.
// Returns nil if currently connected.
func (cs *clientState) Err() error {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	if (cs.connectedAt == 0) != (cs.closeErr != nil) {
		panic("assertion failed: bug in natiu-mqtt clientState implementation")
	}
	return cs.closeErr
}

// PendingResponse returns true if the client is waiting on the server for a response.
func (cs *clientState) PendingResponse() bool {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	return cs.closeErr == nil && (len(cs.pendingSubs.TopicFilters) > 0 || cs.pendingPingreq != 0)
}

func (cs *clientState) AwaitingPingresp() bool {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	return cs.pendingPingresp != 0
}

func (cs *clientState) AwaitingSuback() bool {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	return cs.awaitingSuback()
}
func (cs *clientState) awaitingSuback() bool {
	return len(cs.pendingSubs.TopicFilters) > 0
}

func (cs *clientState) RegisterSubscribe(vsub VariablesSubscribe) error {
	if len(vsub.TopicFilters) == 0 {
		return errors.New("need at least one topic to subscribe")
	}
	cs.mu.Lock()
	defer cs.mu.Unlock()
	if cs.awaitingSuback() {
		return errors.New("tried to register subscribe while awaiting suback")
	}
	cs.pendingSubs = vsub.Copy()
	return nil
}

func (cs *clientState) LastPingTime() time.Time {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	return cs.nano2time(cs.pendingPingresp)
}

func (cs *clientState) PendingSublen() int {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	return len(cs.pendingSubs.TopicFilters)
}

func (cs *clientState) ConnectedAt() time.Time {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	return cs.nano2time(cs.connectedAt)
}

func (cs *clientState) LastTx() time.Time {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	return cs.nano2time(cs.lastTx)
}

func (cs *clientState) PingSent() {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.pendingPingresp = cs.nanotime()
}

func (cs *clientState) LastRx() time.Time {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	return cs.nano2time(cs.lastRx)
}

var zerotime time.Time = time.Unix(0, 0)

func (*clientState) nano2time(i int64) time.Time {
	return zerotime.Add(time.Duration(i))
}
