package mqtt

import (
	"testing"
	"time"
)

const orchRetry = time.Second

// orchImpls lists every QoSOrchestrator implementation under test. Each factory
// wires a deterministic clock driven by the *now pointer the test advances.
var orchImpls = []struct {
	name string
	make func(t *testing.T, now *int64) QoSOrchestrator
}{
	{"NoAlloc", func(t *testing.T, now *int64) QoSOrchestrator {
		var o OrchestratorNoAlloc
		err := o.Configure(OrchestratorNoAllocConfig{
			MaxOutbound: 4, MaxInboundQoS2: 4, MaxPendingResp: 4,
			MaxTopicLen: 32, MaxPayloadLen: 64, RetryInterval: orchRetry,
			Nanotime: func() int64 { return *now },
		})
		if err != nil {
			t.Fatal(err)
		}
		return &o
	}},
}

// ----- interface-level helpers (no implementation internals) -----

func pubSend(t *testing.T, o QoSOrchestrator, qos QoSLevel, topic, payload string) uint16 {
	t.Helper()
	id, err := o.PacketIdentifier(PacketPublish, qos, len(payload))
	if err != nil {
		t.Fatalf("PacketIdentifier: %v", err)
	}
	if id == 0 {
		t.Fatal("PacketIdentifier returned zero")
	}
	flags, _ := NewPublishFlags(qos, false, false)
	h := newHeader(PacketPublish, flags, 0)
	v := VariablesPublish{TopicName: []byte(topic), PacketIdentifier: id}
	if err := o.OnPublishSent(h, v, []byte(payload)); err != nil {
		t.Fatalf("OnPublishSent: %v", err)
	}
	return id
}

func inPublish(t *testing.T, o QoSOrchestrator, qos QoSLevel, pid uint16) {
	t.Helper()
	flags, _ := NewPublishFlags(qos, false, false)
	if err := o.OnControlPacket(newHeader(PacketPublish, flags, 0), pid); err != nil {
		t.Fatalf("OnControlPacket(inbound PUBLISH): %v", err)
	}
}

func inCtrl(t *testing.T, o QoSOrchestrator, pt PacketType, pid uint16) {
	t.Helper()
	var flags PacketFlags
	if pt == PacketPubrel {
		flags = PacketFlagsPubrelSubUnsub
	}
	if err := o.OnControlPacket(newHeader(pt, flags, 2), pid); err != nil {
		t.Fatalf("OnControlPacket(%s): %v", pt, err)
	}
}

func wantNone(t *testing.T, o QoSOrchestrator) {
	t.Helper()
	if act, ok := o.NextRetransmit(); ok {
		t.Fatalf("expected no action, got %+v", act)
	}
}

func wantCtrl(t *testing.T, o QoSOrchestrator, pt PacketType, pid uint16) {
	t.Helper()
	act, ok := o.NextRetransmit()
	if !ok {
		t.Fatalf("expected control %s id=%d, got none", pt, pid)
	}
	if !act.IsControl || act.ControlPacketType != pt || act.ControlPacketID != pid {
		t.Fatalf("expected control %s id=%d, got %+v", pt, pid, act)
	}
}

func wantPublishDUP(t *testing.T, o QoSOrchestrator, pid uint16, topic, payload string) {
	t.Helper()
	act, ok := o.NextRetransmit()
	if !ok {
		t.Fatalf("expected PUBLISH retransmit id=%d, got none", pid)
	}
	if act.IsControl {
		t.Fatalf("expected PUBLISH retransmit, got control %+v", act)
	}
	if !act.Header.Flags().Dup() {
		t.Error("retransmitted PUBLISH must have DUP set")
	}
	if act.Variables.PacketIdentifier != pid {
		t.Errorf("retransmit PID = %d, want %d", act.Variables.PacketIdentifier, pid)
	}
	if string(act.Variables.TopicName) != topic {
		t.Errorf("retransmit topic = %q, want %q", act.Variables.TopicName, topic)
	}
	if string(act.Payload) != payload {
		t.Errorf("retransmit payload = %q, want %q", act.Payload, payload)
	}
}

// ----- table of scenarios run against every implementation -----

var orchCases = []struct {
	name string
	run  func(t *testing.T, o QoSOrchestrator, now *int64)
}{
	{"qos0_rejected", func(t *testing.T, o QoSOrchestrator, now *int64) {
		if _, err := o.PacketIdentifier(PacketPublish, QoS0, 4); err == nil {
			t.Fatal("expected error allocating identifier for QoS0")
		}
	}},

	{"qos1_happy", func(t *testing.T, o QoSOrchestrator, now *int64) {
		id := pubSend(t, o, QoS1, "a/b", "hello")
		wantNone(t, o) // Not yet due for retransmit.
		inCtrl(t, o, PacketPuback, id)
		wantNone(t, o)
	}},

	{"qos1_retransmit", func(t *testing.T, o QoSOrchestrator, now *int64) {
		id := pubSend(t, o, QoS1, "a/b", "hello")
		*now += int64(orchRetry) // Past the retry interval.
		wantPublishDUP(t, o, id, "a/b", "hello")
		inCtrl(t, o, PacketPuback, id)
		wantNone(t, o)
	}},

	{"qos2_happy", func(t *testing.T, o QoSOrchestrator, now *int64) {
		id := pubSend(t, o, QoS2, "t", "hi")
		inCtrl(t, o, PacketPubrec, id)
		wantCtrl(t, o, PacketPubrel, id) // PUBREL emitted in response to PUBREC.
		inCtrl(t, o, PacketPubcomp, id)
		wantNone(t, o)
	}},

	{"qos2_pubrel_retransmit", func(t *testing.T, o QoSOrchestrator, now *int64) {
		id := pubSend(t, o, QoS2, "t", "hi")
		inCtrl(t, o, PacketPubrec, id)
		wantCtrl(t, o, PacketPubrel, id) // First PUBREL.
		*now += int64(orchRetry)
		wantCtrl(t, o, PacketPubrel, id) // Retransmitted PUBREL.
		inCtrl(t, o, PacketPubcomp, id)
		wantNone(t, o)
	}},

	{"inbound_qos1", func(t *testing.T, o QoSOrchestrator, now *int64) {
		inPublish(t, o, QoS1, 42)
		wantCtrl(t, o, PacketPuback, 42)
		wantNone(t, o)
	}},

	{"inbound_qos2", func(t *testing.T, o QoSOrchestrator, now *int64) {
		inPublish(t, o, QoS2, 42)
		wantCtrl(t, o, PacketPubrec, 42)
		inCtrl(t, o, PacketPubrel, 42)
		wantCtrl(t, o, PacketPubcomp, 42)
		wantNone(t, o)
	}},

	{"pid_unique_nonzero", func(t *testing.T, o QoSOrchestrator, now *int64) {
		seen := make(map[uint16]bool)
		for i := 0; i < 4; i++ {
			id, err := o.PacketIdentifier(PacketPublish, QoS1, 1)
			if err != nil {
				t.Fatalf("PacketIdentifier #%d: %v", i, err)
			}
			if id == 0 {
				t.Fatalf("PacketIdentifier #%d returned zero", i)
			}
			if seen[id] {
				t.Fatalf("duplicate packet identifier %d", id)
			}
			seen[id] = true
		}
	}},

	{"reset_clears", func(t *testing.T, o QoSOrchestrator, now *int64) {
		pubSend(t, o, QoS1, "a", "x")
		o.Reset()
		*now += int64(orchRetry)
		wantNone(t, o) // No retransmit for the discarded in-flight message.
		// A fresh send after Reset still works.
		id := pubSend(t, o, QoS1, "b", "y")
		*now += int64(orchRetry)
		wantPublishDUP(t, o, id, "b", "y")
	}},
}

func TestOrchestratorConformance(t *testing.T) {
	for _, impl := range orchImpls {
		for _, tc := range orchCases {
			t.Run(impl.name+"/"+tc.name, func(t *testing.T) {
				now := int64(0)
				o := impl.make(t, &now)
				tc.run(t, o, &now)
			})
		}
	}
}

// TestOrchestratorNoAlloc_ZeroAlloc asserts the no-alloc guarantee specific to
// OrchestratorNoAlloc: no allocations occur after Configure.
func TestOrchestratorNoAlloc_ZeroAlloc(t *testing.T) {
	var now int64
	var o OrchestratorNoAlloc
	err := o.Configure(OrchestratorNoAllocConfig{
		MaxOutbound: 4, MaxInboundQoS2: 4, MaxPendingResp: 4,
		MaxTopicLen: 32, MaxPayloadLen: 64, RetryInterval: orchRetry,
		Nanotime: func() int64 { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	flags, _ := NewPublishFlags(QoS1, false, false)
	topic := []byte("a/b")
	payload := []byte("hello")
	allocs := testing.AllocsPerRun(100, func() {
		id, _ := o.PacketIdentifier(PacketPublish, QoS1, len(payload))
		o.OnPublishSent(newHeader(PacketPublish, flags, 0), VariablesPublish{TopicName: topic, PacketIdentifier: id}, payload)
		o.OnControlPacket(newHeader(PacketPuback, 0, 2), id)
	})
	if allocs != 0 {
		t.Fatalf("expected 0 allocs after Configure, got %v", allocs)
	}
}
