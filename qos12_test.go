package mqtt

import (
	"context"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

// ----- minimal ltesto-style scheduler for deterministic goroutine handoff -----

type sched struct {
	t                  testing.TB
	goroYieldSignal    chan struct{}
	goroContinueSignal chan struct{}
	finishChan         chan error
	finishCalled       atomic.Bool
	coroCalls          atomic.Int32
	timeout            time.Duration
}

func newSched(t testing.TB) *sched {
	return &sched{
		t:                  t,
		goroYieldSignal:    make(chan struct{}),
		goroContinueSignal: make(chan struct{}),
		finishChan:         make(chan error, 1),
		timeout:            time.Second,
	}
}

func (ss *sched) AwaitGoroYield() {
	select {
	case <-ss.goroYieldSignal:
	case <-time.After(ss.timeout):
		ss.t.Fatal("timeout waiting for goroutine to yield")
	}
}

func (ss *sched) YieldToGoro() {
	select {
	case ss.goroContinueSignal <- struct{}{}:
	case <-time.After(ss.timeout):
		ss.t.Fatal("timeout yielding to goroutine")
	}
}

func (ss *sched) Done() <-chan error {
	if ss.finishCalled.CompareAndSwap(false, true) {
		return ss.finishChan
	}
	panic("Done called twice")
}

func (ss *sched) Goro() schedGoro {
	if !ss.coroCalls.CompareAndSwap(0, 1) {
		panic("only one goroutine supported")
	}
	return schedGoro{ss: ss}
}

type schedGoro struct{ ss *sched }

func (g schedGoro) Yield() {
	ss := g.ss
	select {
	case ss.goroYieldSignal <- struct{}{}:
	case <-time.After(ss.timeout):
		ss.t.Fatal("timeout signalling yield")
	}
	select {
	case <-ss.goroContinueSignal:
	case <-time.After(ss.timeout):
		ss.t.Fatal("timeout waiting for continue")
	}
}

func (g schedGoro) FinishWithErr(err error) {
	ss := g.ss
	if len(ss.finishChan) != 0 {
		ss.t.Fatal("FinishWithErr called more than once")
	}
	ss.finishChan <- err
}

func (g schedGoro) Finish() { g.FinishWithErr(nil) }

// ----- behavioural tests using OrchestratorNoAlloc -----

func newTestOrchestrator(t testing.TB) *OrchestratorNoAlloc {
	t.Helper()
	var o OrchestratorNoAlloc
	err := o.Configure(OrchestratorNoAllocConfig{
		MaxOutbound: 4, MaxInboundQoS2: 4, MaxPendingResp: 4,
		MaxTopicLen: 32, MaxPayloadLen: 64, RetryInterval: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	return &o
}

// outInflight counts non-free outbound slots in the orchestrator.
func outInflight(o *OrchestratorNoAlloc) int {
	n := 0
	for i := range o.out {
		if o.out[i].state != stateFree {
			n++
		}
	}
	return n
}

func TestOrchestratorNoAlloc_PacketIdentifier(t *testing.T) {
	o := newTestOrchestrator(t)
	id1, err := o.PacketIdentifier(PacketPublish, QoS1, 10)
	if err != nil || id1 == 0 {
		t.Fatalf("expected valid id, got %d %v", id1, err)
	}
	id2, _ := o.PacketIdentifier(PacketPublish, QoS1, 10)
	if id2 != id1+1 {
		t.Fatalf("expected sequential ids, got %d then %d", id1, id2)
	}
}

// TestClientPublishQoS1Pipe drives a real QoS1 PUBLISH over an in-memory pipe and
// verifies the orchestrator tracks the in-flight message and frees it once the
// broker's PUBACK is processed by the Client's HandleNext loop.
func TestClientPublishQoS1Pipe(t *testing.T) {
	const testTimeout = 3 * time.Second
	clientEnd, brokerEnd := net.Pipe()
	defer clientEnd.Close()
	defer brokerEnd.Close()

	o := newTestOrchestrator(t)
	c := NewClient(ClientConfig{Orchestrator: o})
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	go runPubackBroker(ctx, brokerEnd)

	var varconn VariablesConnect
	varconn.SetDefaultMQTT([]byte("natiu-qos1"))
	if err := c.Connect(ctx, clientEnd, &varconn); err != nil {
		t.Fatal(err)
	}

	flags, _ := NewPublishFlags(QoS1, false, false)
	if err := c.PublishPayload(flags, VariablesPublish{TopicName: []byte("abc")}, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	if got := outInflight(o); got != 1 {
		t.Fatalf("expected 1 in-flight after QoS1 publish, got %d", got)
	}

	// Pump until the PUBACK frees the in-flight entry. A read deadline guarantees
	// HandleNext cannot block forever if the PUBACK never arrives.
	deadline := time.Now().Add(testTimeout)
	if err := clientEnd.SetReadDeadline(deadline); err != nil {
		t.Fatal(err)
	}
	for outInflight(o) != 0 {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for PUBACK")
		}
		if err := c.HandleNext(); err != nil {
			t.Fatal(err)
		}
	}
}

// runPubackBroker accepts one connection, CONNACKs, and replies to every QoS1
// PUBLISH with a PUBACK. The PUBACK is sent from the loop after ReadNextPacket
// has fully consumed the PUBLISH (including its payload); replying from inside
// OnPub would deadlock the synchronous pipe (both ends blocked writing). Exits
// when the pipe closes or ctx ends.
func runPubackBroker(ctx context.Context, conn net.Conn) {
	defer conn.Close()
	rxtx, _ := NewRxTx(conn, DecoderNoAlloc{UserBuffer: make([]byte, 1024)})
	var pendingPuback uint16
	rxtx.RxCallbacks = RxCallbacks{
		OnConnect: func(r *Rx, vc *VariablesConnect) error {
			return rxtx.WriteConnack(VariablesConnack{ReturnCode: 0})
		},
		OnPub: func(rx *Rx, vp VariablesPublish, r io.Reader) error {
			if rx.LastReceivedHeader.Flags().QoS() == QoS1 {
				pendingPuback = vp.PacketIdentifier
			}
			return nil
		},
	}
	for ctx.Err() == nil {
		if _, err := rxtx.ReadNextPacket(); err != nil {
			return
		}
		if pendingPuback != 0 {
			id := pendingPuback
			pendingPuback = 0
			if err := rxtx.WriteIdentified(PacketPuback, id); err != nil {
				return
			}
		}
	}
}
