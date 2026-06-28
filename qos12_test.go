package mqtt

import (
	"bytes"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

// QoSOrchestratorHeap is a freely-allocating implementation of QoSOrchestrator
// intended for tests. It is defined here so it is only visible to tests.
type QoSOrchestratorHeap struct {
	inflight map[uint16]flight
	nextID   uint16
	nanotime func() int64
}

type flight struct {
	h       Header
	v       VariablesPublish
	payload []byte
	sentAt  int64
	qos     QoSLevel
}

func (o *QoSOrchestratorHeap) PacketIdentifier(pt PacketType, qos QoSLevel, payloadSize int) (uint16, error) {
	if o.inflight == nil {
		o.inflight = make(map[uint16]flight)
	}
	if o.nextID == 0 {
		o.nextID = 1
	}
	for i := 0; i < 65536; i++ {
		id := o.nextID
		o.nextID++
		if o.nextID == 0 {
			o.nextID = 1
		}
		if _, ok := o.inflight[id]; !ok {
			return id, nil
		}
	}
	return 0, ErrPacketIDExhausted
}

func (o *QoSOrchestratorHeap) OnPublishSent(h Header, v VariablesPublish, payload []byte) error {
	if o.inflight == nil {
		o.inflight = make(map[uint16]flight)
	}
	qos := h.Flags().QoS()
	o.inflight[v.PacketIdentifier] = flight{
		h:       h,
		v:       v,
		payload: append([]byte(nil), payload...),
		sentAt:  o.nanotime(),
		qos:     qos,
	}
	return nil
}

func (o *QoSOrchestratorHeap) OnControlPacket(h Header, packetID uint16) error {
	tp := h.Type()
	switch tp {
	case PacketPuback, PacketPubcomp:
		delete(o.inflight, packetID)
	case PacketPubrec:
	case PacketPubrel:
		delete(o.inflight, packetID)
	}
	return nil
}

func (o *QoSOrchestratorHeap) NextRetransmit() (RetransmitAction, bool) {
	now := o.nanotime()
	for id, f := range o.inflight {
		if now-f.sentAt > int64(5*time.Second) {
			act := RetransmitAction{
				Header:    f.h,
				Variables: f.v,
				Payload:   f.payload,
			}
			act.Header.firstByte |= 0x08 // DUP
			return act, true
		}
		_ = id
	}
	return RetransmitAction{}, false
}

func (o *QoSOrchestratorHeap) Reset() {
	o.inflight = make(map[uint16]flight)
	o.nextID = 0
	o.nanotime = func() int64 {
		return time.Now().UnixNano()
	}
}

// ----- minimal test server -----

type testServer struct {
	net.Conn
	mu     sync.Mutex
	recv   [][]byte
	sendQ  [][]byte
	closed bool
	onPub  func(h Header, v VariablesPublish, payload []byte)
}

func newTestServer() *testServer {
	c1, c2 := net.Pipe()
	s := &testServer{Conn: c1}
	go s.loop(c2)
	return s
}

func (s *testServer) loop(c net.Conn) {
	defer c.Close()
	for {
		hdr, _, err := DecodeHeader(c)
		if err != nil {
			return
		}
		if hdr.Type() == PacketPublish {
			qos := hdr.Flags().QoS()
			vp, _, _ := decodePublishTest(c, qos) // simplified decode
			payload := make([]byte, int(hdr.RemainingLength)-int(vp.Size(qos)))
			io.ReadFull(c, payload)
			s.mu.Lock()
			s.recv = append(s.recv, payload)
			s.mu.Unlock()
			if s.onPub != nil {
				s.onPub(hdr, vp, payload)
			}
			if qos == QoS1 {
				// send PUBACK
				ack := make([]byte, 4)
				ack[0] = byte(PacketPuback) << 4
				ack[1] = 2
				ack[2] = byte(vp.PacketIdentifier >> 8)
				ack[3] = byte(vp.PacketIdentifier)
				s.mu.Lock()
				s.sendQ = append(s.sendQ, ack)
				s.mu.Unlock()
			}
		}
	}
}

func (s *testServer) Write(b []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return 0, io.EOF
	}
	s.sendQ = append(s.sendQ, append([]byte(nil), b...))
	return len(b), nil
}

func (s *testServer) Read(b []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.sendQ) == 0 {
		return 0, nil // simulate no data, test will timeout
	}
	data := s.sendQ[0]
	s.sendQ = s.sendQ[1:]
	copy(b, data)
	return len(data), nil
}

func (s *testServer) Close() error {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	return s.Conn.Close()
}

// decodePublishTest is a tiny helper for the test server only.
func decodePublishTest(r io.Reader, qos QoSLevel) (VariablesPublish, int, error) {
	topic, n, _ := decodeMQTTString(r, make([]byte, 256))
	var pi uint16
	if qos > 0 {
		var ng int
		pi, ng, _ = decodeUint16(r)
		n += ng
	}
	return VariablesPublish{TopicName: topic, PacketIdentifier: pi}, n, nil
}

// ----- tests -----

func TestQoSOrchestratorHeap_PacketIdentifier(t *testing.T) {
	var o QoSOrchestratorHeap
	o.Reset()
	id1, err := o.PacketIdentifier(PacketPublish, QoS1, 10)
	if err != nil || id1 == 0 {
		t.Fatalf("expected valid id, got %d %v", id1, err)
	}
	id2, _ := o.PacketIdentifier(PacketPublish, QoS1, 10)
	if id2 != id1+1 {
		t.Fatalf("expected sequential ids, got %d then %d", id1, id2)
	}
}

func TestClientQoS12_PublishQoS1(t *testing.T) {
	base := NewClient(ClientConfig{})
	var o QoSOrchestratorHeap
	o.Reset()
	var now int64
	o.nanotime = func() int64 {
		return now
	}
	c := NewClientQoS12(base, ClientQoS12Config{Orchestrator: &o, DefaultQoS: QoS1})

	srv := newTestServer()
	defer srv.Close()

	err := base.StartConnect(srv, &VariablesConnect{ClientID: []byte("test")})
	if err != nil {
		t.Fatal(err)
	}
	// simulate connack
	// (for brevity we assume connected)
	base.cs.onConnect(base.cs.nanotime())

	err = c.PublishPayload(0, VariablesPublish{TopicName: []byte("t")}, []byte("hello"))
	if err != nil {
		t.Fatalf("publish qos1 failed: %v", err)
	}
	// give the server goroutine time to process
	time.Sleep(20 * time.Millisecond)

	srv.mu.Lock()
	recvLen := len(srv.recv)
	srv.mu.Unlock()
	if recvLen == 0 {
		t.Fatal("server did not receive the publish")
	}
}

// Additional behavioural tests can be added similarly.
var _ = errors.New // silence unused in some builds
var _ = bytes.Buffer{}
