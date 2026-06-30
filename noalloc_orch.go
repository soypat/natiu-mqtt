package mqtt

import (
	"errors"
	"time"
)

// OrchestratorNoAlloc is a memory-mindful QoSOrchestrator implementation for
// memory-constrained systems. All storage is allocated in [OrchestratorNoAlloc.Configure];
// no allocations occur afterwards and no maps are used.
//
// Layout (see plan):
//   - Separate fixed pools for outbound and inbound-QoS2 in-flight state.
//   - Each pool is a tombstone array: a slot is free when state == stateFree,
//     so a slot's index permanently owns its reserved topic/payload buffer
//     region and bytes never move.
//   - Topic and payload bytes are copied into per-slot reserved regions on send
//     (the orchestrator owns them, decoupled from caller buffer lifetime).
//   - Linear scan is used for PID->slot lookup; the in-flight window is small.
//
// This type defines the data structure only; the QoS state-machine algorithms
// (OnPublishSent/OnControlPacket/NextRetransmit/PacketIdentifier) are implemented
// separately.
type OrchestratorNoAlloc struct {
	out        []outFlight // len == cfg.MaxOutbound; tombstone array.
	in         []inFlight  // len == cfg.MaxInboundQoS2; tombstone array.
	resp       []ctrlResp  // ring buffer of len cfg.MaxPendingResp.
	topicBuf   []byte      // MaxOutbound*MaxTopicLen; region i = [i*MaxTopicLen:(i+1)*MaxTopicLen].
	payloadBuf []byte      // MaxOutbound*MaxPayloadLen; region i likewise.

	_nanotime func() int64 // Time source; nil uses time.Now().UnixNano().

	maxTopicLen int
	maxPayload  int
	retry       time.Duration
	rHead       uint16 // resp ring read index, in [0,len(resp)).
	rTail       uint16 // resp ring write index, in [0,len(resp)).
	nextID      uint16 // monotonic packet identifier allocation cursor.
	maxRetries  uint16
}

// qosState identifies the lifecycle position of an in-flight slot. The zero
// value stateFree marks an unused (tombstoned) slot.
type qosState uint8

const (
	stateFree          qosState = iota // Slot unused.
	stateReserved                      // Outbound: PID allocated, awaiting OnPublishSent (unarmed).
	stateAwaitPuback                   // Outbound QoS1: PUBLISH sent, awaiting PUBACK.
	stateAwaitPubrec                   // Outbound QoS2: PUBLISH sent, awaiting PUBREC (payload held).
	stateAwaitPubcomp                  // Outbound QoS2: PUBREL sent, awaiting PUBCOMP (payload droppable).
	stateInAwaitPubrel                 // Inbound  QoS2: PUBREC sent, awaiting PUBREL.
)

// outFlight is a fat outbound in-flight slot. Fields are ordered int64, then
// uint32, then uint16/uint8 to minimize struct padding. Its topic and payload
// bytes live in the orchestrator's reserved regions for this slot's index.
type outFlight struct {
	sentAt   int64       // Nanotime of last (re)transmission; timeout source.
	topicLen uint32      // Bytes used of this slot's reserved topic region.
	payLen   uint32      // Bytes used of this slot's reserved payload region.
	packetID uint16      // Non-zero packet identifier while in use.
	flags    PacketFlags // Original PUBLISH flags; DUP added on retransmit.
	state    qosState
	retries  uint16
}

// inFlight is a slim inbound-QoS2 slot. No topic/payload is stored: the inbound
// payload is delivered live through the OnPub reader at receipt; only the packet
// identifier is needed for duplicate detection and the PUBREC/PUBCOMP handshake.
type inFlight struct {
	sentAt   int64 // Nanotime of last PUBREC (re)transmission.
	packetID uint16
	state    qosState
}

// ctrlResp is a transient one-shot control response queued for transmission,
// e.g. the PUBACK for an inbound QoS1 PUBLISH which keeps no lasting slot.
type ctrlResp struct {
	packetID uint16
	pt       PacketType
}

// OrchestratorNoAllocConfig sizes the fixed storage of a [OrchestratorNoAlloc]. All
// capacities are reserved up front.
type OrchestratorNoAllocConfig struct {
	// MaxOutbound is the maximum number of concurrent outbound QoS>0 PUBLISHes.
	MaxOutbound int
	// MaxInboundQoS2 is the maximum number of concurrent inbound QoS2 flows.
	MaxInboundQoS2 int
	// MaxPendingResp is the capacity of the transient control-response ring.
	MaxPendingResp int
	// MaxTopicLen is the largest topic (bytes) retained per outbound slot.
	MaxTopicLen int
	// MaxPayloadLen is the largest payload (bytes) retained per outbound slot.
	MaxPayloadLen int
	// RetryInterval is the time before an unacknowledged packet is retransmitted.
	RetryInterval time.Duration
	// MaxRetries is the number of retransmissions before giving up. Zero means
	// retry indefinitely.
	MaxRetries uint16
	// Nanotime returns the current time in nanoseconds. When nil,
	// time.Now().UnixNano() is used. Identical semantics to ClientConfig.Nanotime;
	// useful for deterministic tests that simulate time.
	Nanotime func() int64
}

// Configure sizes (or resizes) the orchestrator's fixed storage and resets it to
// a clean state. Backing arrays are reused in place when their capacity already
// suffices, so a second Configure with equal-or-smaller sizes allocates nothing.
func (qmem *OrchestratorNoAlloc) Configure(cfg OrchestratorNoAllocConfig) error {
	switch {
	case cfg.MaxOutbound <= 0:
		return errors.New("natiu-mqtt: MaxOutbound must be positive")
	case cfg.MaxInboundQoS2 < 0:
		return errors.New("natiu-mqtt: MaxInboundQoS2 must be non-negative")
	case cfg.MaxPendingResp <= 0:
		return errors.New("natiu-mqtt: MaxPendingResp must be positive")
	case cfg.MaxTopicLen <= 0:
		return errors.New("natiu-mqtt: MaxTopicLen must be positive")
	case cfg.MaxPayloadLen < 0:
		return errors.New("natiu-mqtt: MaxPayloadLen must be non-negative")
	case cfg.RetryInterval <= 0:
		return errors.New("natiu-mqtt: RetryInterval must be positive")
	}
	sliceReuse(&qmem.out, cfg.MaxOutbound)
	sliceReuse(&qmem.in, cfg.MaxInboundQoS2)
	// The response ring keeps one slot empty to distinguish full from empty, so
	// it is sized one larger than the requested usable capacity.
	sliceReuse(&qmem.resp, cfg.MaxPendingResp+1)
	sliceReuse(&qmem.topicBuf, cfg.MaxOutbound*cfg.MaxTopicLen)
	sliceReuse(&qmem.payloadBuf, cfg.MaxOutbound*cfg.MaxPayloadLen)
	qmem._nanotime = cfg.Nanotime
	qmem.maxTopicLen = cfg.MaxTopicLen
	qmem.maxPayload = cfg.MaxPayloadLen
	qmem.retry = cfg.RetryInterval
	qmem.maxRetries = cfg.MaxRetries
	qmem.Reset()
	return nil
}

// sliceReuse reuses *s's backing array when it already has capacity for n,
// otherwise allocates a new slice. The resulting slice has length n.
func sliceReuse[T any](s *[]T, n int) {
	if cap(*s) < n {
		*s = make([]T, n)
	} else {
		*s = (*s)[:n]
	}
}

// dupFlag is the PUBLISH DUP bit, set on every retransmission [MQTT-3.3.1-1].
const dupFlag = PacketFlags(1 << 3)

// Compile-time assertion that OrchestratorNoAlloc satisfies QoSOrchestrator.
var _ QoSOrchestrator = (*OrchestratorNoAlloc)(nil)

func (q *OrchestratorNoAlloc) nanotime() int64 {
	if q._nanotime != nil {
		return q._nanotime()
	}
	return time.Now().UnixNano()
}

// PacketIdentifier reserves an outbound in-flight slot and returns a fresh
// non-zero packet identifier for a QoS1/QoS2 PUBLISH. The slot's payload/topic
// bytes are filled in by the subsequent OnPublishSent call.
func (q *OrchestratorNoAlloc) PacketIdentifier(pt PacketType, qos QoSLevel, payloadSize int) (uint16, error) {
	if qos != QoS1 && qos != QoS2 {
		return 0, errors.New("natiu-mqtt: orchestrator allocates identifiers for QoS1/QoS2 only")
	}
	if payloadSize > q.maxPayload {
		return 0, errors.New("natiu-mqtt: payload exceeds MaxPayloadLen")
	}
	slot := q.freeOutSlot()
	if slot < 0 {
		return 0, errors.New("natiu-mqtt: outbound in-flight window full")
	}
	id, err := q.allocID()
	if err != nil {
		return 0, err
	}
	// Reserve the slot. It stays unarmed (stateReserved) until OnPublishSent fills
	// the data and transitions it to an await state.
	q.out[slot] = outFlight{packetID: id, state: stateReserved}
	return id, nil
}

// OnPublishSent records the just-transmitted QoS>0 PUBLISH bytes into the slot
// reserved by PacketIdentifier and arms its retransmission timer.
func (q *OrchestratorNoAlloc) OnPublishSent(h Header, v VariablesPublish, payload []byte) error {
	slot := q.findOut(v.PacketIdentifier, stateReserved)
	if slot < 0 {
		return errors.New("natiu-mqtt: OnPublishSent for unknown packet identifier")
	}
	if len(v.TopicName) > q.maxTopicLen {
		return errors.New("natiu-mqtt: topic exceeds MaxTopicLen")
	}
	if len(payload) > q.maxPayload {
		return errors.New("natiu-mqtt: payload exceeds MaxPayloadLen")
	}
	e := &q.out[slot]
	toff := slot * q.maxTopicLen
	poff := slot * q.maxPayload
	e.topicLen = uint32(copy(q.topicBuf[toff:toff+q.maxTopicLen], v.TopicName))
	e.payLen = uint32(copy(q.payloadBuf[poff:poff+q.maxPayload], payload))
	e.flags = h.Flags()
	e.sentAt = q.nanotime()
	e.retries = 0
	if h.Flags().QoS() == QoS2 {
		e.state = stateAwaitPubrec
	} else {
		e.state = stateAwaitPuback
	}
	return nil
}

// OnControlPacket advances the QoS state machine for an inbound control packet
// or inbound QoS>0 PUBLISH, enqueueing any immediate response.
func (q *OrchestratorNoAlloc) OnControlPacket(h Header, packetID uint16) error {
	switch h.Type() {
	case PacketPuback: // Outbound QoS1 acknowledged: done.
		if s := q.findOut(packetID, stateAwaitPuback); s >= 0 {
			q.freeOut(s)
		}
	case PacketPubrec: // Outbound QoS2: PUBLISH received, must send PUBREL.
		if s := q.findOut(packetID, stateAwaitPubrec); s >= 0 {
			e := &q.out[s]
			e.state = stateAwaitPubcomp
			e.payLen = 0 // Payload no longer needed once PUBREC arrives.
			e.sentAt = 0 // Emit PUBREL on the next NextRetransmit.
			e.retries = 0
		}
	case PacketPubcomp: // Outbound QoS2 complete: done.
		if s := q.findOut(packetID, stateAwaitPubcomp); s >= 0 {
			q.freeOut(s)
		}
	case PacketPubrel: // Inbound QoS2: peer released, respond PUBCOMP.
		if s := q.findIn(packetID, stateInAwaitPubrel); s >= 0 {
			q.freeIn(s)
		}
		return q.enqueueResp(PacketPubcomp, packetID)
	case PacketPublish: // Inbound QoS>0 PUBLISH.
		switch h.Flags().QoS() {
		case QoS1:
			return q.enqueueResp(PacketPuback, packetID)
		case QoS2:
			if s := q.findIn(packetID, stateInAwaitPubrel); s >= 0 {
				q.in[s].sentAt = 0 // Duplicate: re-arm PUBREC emission, do not double-track.
				return nil
			}
			s := q.freeInSlot()
			if s < 0 {
				return errors.New("natiu-mqtt: inbound QoS2 window full")
			}
			q.in[s] = inFlight{packetID: packetID, state: stateInAwaitPubrel}
		}
	}
	return nil
}

// NextRetransmit returns the next wire action the Client should perform: a
// queued one-shot response first, then any due (re)transmission.
func (q *OrchestratorNoAlloc) NextRetransmit() (RetransmitAction, bool) {
	// 1. Transient one-shot control responses (PUBACK/PUBCOMP).
	if c, ok := q.dequeueResp(); ok {
		return RetransmitAction{IsControl: true, ControlPacketType: c.pt, ControlPacketID: c.packetID}, true
	}
	now := q.nanotime()
	retry := int64(q.retry)
	// 2. Outbound PUBLISH retransmits and PUBREL emission.
	for i := range q.out {
		e := &q.out[i]
		switch e.state {
		case stateAwaitPuback, stateAwaitPubrec:
			// First transmission was done by the Client (OnPublishSent armed sentAt);
			// only retransmit once the retry interval elapses.
			if now-e.sentAt < retry {
				continue
			}
			if q.maxRetries != 0 && e.retries >= q.maxRetries {
				q.freeOut(i)
				continue
			}
			e.sentAt = now
			e.retries++
			return q.buildPublish(i), true
		case stateAwaitPubcomp:
			// Orchestrator owns PUBREL emission, including the first (sentAt==0).
			if e.sentAt != 0 && now-e.sentAt < retry {
				continue
			}
			if q.maxRetries != 0 && e.retries >= q.maxRetries {
				q.freeOut(i)
				continue
			}
			e.sentAt = now
			e.retries++
			return RetransmitAction{IsControl: true, ControlPacketType: PacketPubrel, ControlPacketID: e.packetID}, true
		}
	}
	// 3. Inbound QoS2 PUBREC emission (first and retransmits).
	for i := range q.in {
		e := &q.in[i]
		if e.state != stateInAwaitPubrel {
			continue
		}
		if e.sentAt != 0 && now-e.sentAt < retry {
			continue
		}
		e.sentAt = now
		return RetransmitAction{IsControl: true, ControlPacketType: PacketPubrec, ControlPacketID: e.packetID}, true
	}
	return RetransmitAction{}, false
}

// Reset clears all in-flight state and pending responses.
func (q *OrchestratorNoAlloc) Reset() {
	for i := range q.out {
		q.out[i] = outFlight{}
	}
	for i := range q.in {
		q.in[i] = inFlight{}
	}
	q.rHead = 0
	q.rTail = 0
	q.nextID = 0
}

// buildPublish constructs a PUBLISH RetransmitAction from outbound slot i, with
// the DUP bit set and topic/payload pointing into the slot's reserved regions.
func (q *OrchestratorNoAlloc) buildPublish(i int) RetransmitAction {
	e := &q.out[i]
	qos := e.flags.QoS()
	toff := i * q.maxTopicLen
	poff := i * q.maxPayload
	v := VariablesPublish{
		TopicName:        q.topicBuf[toff : toff+int(e.topicLen)],
		PacketIdentifier: e.packetID,
	}
	payload := q.payloadBuf[poff : poff+int(e.payLen)]
	flags := e.flags | dupFlag
	h := newHeader(PacketPublish, flags, uint32(v.Size(qos)+int(e.payLen)))
	return RetransmitAction{Header: h, Variables: v, Payload: payload}
}

// allocID returns a fresh non-zero packet identifier not currently used by any
// outbound in-flight slot.
func (q *OrchestratorNoAlloc) allocID() (uint16, error) {
	for i := 0; i <= 0xffff; i++ {
		q.nextID++
		if q.nextID == 0 {
			q.nextID = 1
		}
		if !q.idInUse(q.nextID) {
			return q.nextID, nil
		}
	}
	return 0, ErrPacketIDExhausted
}

func (q *OrchestratorNoAlloc) idInUse(id uint16) bool {
	for i := range q.out {
		if q.out[i].state != stateFree && q.out[i].packetID == id {
			return true
		}
	}
	return false
}

func (q *OrchestratorNoAlloc) findOut(id uint16, state qosState) int {
	for i := range q.out {
		if q.out[i].state == state && q.out[i].packetID == id {
			return i
		}
	}
	return -1
}

func (q *OrchestratorNoAlloc) findIn(id uint16, state qosState) int {
	for i := range q.in {
		if q.in[i].state == state && q.in[i].packetID == id {
			return i
		}
	}
	return -1
}

func (q *OrchestratorNoAlloc) freeOutSlot() int {
	for i := range q.out {
		if q.out[i].state == stateFree {
			return i
		}
	}
	return -1
}

func (q *OrchestratorNoAlloc) freeInSlot() int {
	for i := range q.in {
		if q.in[i].state == stateFree {
			return i
		}
	}
	return -1
}

func (q *OrchestratorNoAlloc) freeOut(i int) { q.out[i] = outFlight{} }
func (q *OrchestratorNoAlloc) freeIn(i int)  { q.in[i] = inFlight{} }

// enqueueResp pushes a one-shot control response onto the ring. The ring keeps
// one slot empty, so it is full when advancing rTail would meet rHead.
func (q *OrchestratorNoAlloc) enqueueResp(pt PacketType, id uint16) error {
	n := uint16(len(q.resp))
	if (q.rTail+1)%n == q.rHead {
		return errors.New("natiu-mqtt: pending response ring full")
	}
	q.resp[q.rTail] = ctrlResp{packetID: id, pt: pt}
	q.rTail = (q.rTail + 1) % n
	return nil
}

func (q *OrchestratorNoAlloc) dequeueResp() (ctrlResp, bool) {
	if q.rHead == q.rTail {
		return ctrlResp{}, false
	}
	c := q.resp[q.rHead]
	q.rHead = (q.rHead + 1) % uint16(len(q.resp))
	return c, true
}
