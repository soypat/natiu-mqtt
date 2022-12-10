package mqtt

import (
	"bufio"
	"bytes"
	"io"
	"testing"
)

// Typical packets
var (
	connectHeader              = newHeader(PacketConnect, 0, 0)
	connackHeader              = newHeader(PacketConnack, 0, 0)
	varConnackSuccessNoSession = VariablesConnack{
		AckFlags:   0b0,
		ReturnCode: 0,
	}
)

const (
	qos0Flag = PacketFlags(QoS0 << 1)
	qos1Flag = PacketFlags(QoS1 << 1)
	qos2Flag = PacketFlags(QoS2 << 1)
)

func TestHasPacketIdentifer(t *testing.T) {
	for _, test := range []struct {
		h      Header
		expect bool
	}{
		{h: newHeader(PacketConnect, 0, 0), expect: false},
		{h: newHeader(PacketConnack, 0, 0), expect: false},
		{h: newHeader(PacketPublish, qos0Flag, 0), expect: false},
		{h: newHeader(PacketPublish, qos1Flag, 0), expect: true},
		{h: newHeader(PacketPublish, qos2Flag, 0), expect: true},
		{h: newHeader(PacketPuback, 0, 0), expect: true},
		{h: newHeader(PacketPubrec, 0, 0), expect: true},
		{h: newHeader(PacketPubrel, 0, 0), expect: true},
		{h: newHeader(PacketPubcomp, 0, 0), expect: true},
		{h: newHeader(PacketUnsubscribe, 0, 0), expect: true},
		{h: newHeader(PacketUnsuback, 0, 0), expect: true},
		{h: newHeader(PacketPingreq, 0, 0), expect: false},
		{h: newHeader(PacketPingresp, 0, 0), expect: false},
		{h: newHeader(PacketDisconnect, 0, 0), expect: false},
	} {
		got := test.h.HasPacketIdentifier()
		if got != test.expect {
			t.Errorf("%s: got %v, expected %v", test.h.String(), got, test.expect)
		}
	}
}

func TestRxTxLoopback(t *testing.T) {
	buf := newLoopbackTransport()
	rxtx, err := NewRxTx(buf, DecoderLowmem{make([]byte, 1500)})
	if err != nil {
		t.Fatal(err)
	}
	// Send CONNECT packet over wire.
	var varConn VariablesConnect
	varConn.SetDefaultMQTT([]byte("salamanca"))
	err = rxtx.WriteConnect(connectHeader, &varConn)
	if err != nil {
		t.Fatal(err)
	}
	// We now prepare to receive CONNECT packet on other side.
	rxtx.OnConnect = func(rt *RxTx, vc *VariablesConnect) error {
		if rt.LastReceivedHeader != connectHeader {
			t.Errorf("rxtx header mismatch, rxed:%v, txed:%v", rt.LastReceivedHeader.String(), connectHeader.String())
		}
		return nil
	}
	// Read packet that is on the "wire"
	n, err := rxtx.ReadNextPacket()
	if err != nil {
		t.Fatal(err)
	}
	expectSize := connectHeader.Size() + varConn.Size()
	if n != expectSize {
		t.Errorf("read %v bytes, expected to read %v bytes", n, expectSize)
	}
}

func newLoopbackTransport() io.ReadWriteCloser {
	var _buf bytes.Buffer
	buf := bufio.NewReadWriter(bufio.NewReader(&_buf), bufio.NewWriter(&_buf))
	return &testTransport{buf}
}

type testTransport struct {
	rw *bufio.ReadWriter
}

func (t *testTransport) Close() error {
	t.rw = nil
	return nil
}

func (t *testTransport) Read(p []byte) (int, error) {
	if t.rw == nil {
		return 0, io.ErrClosedPipe
	}
	return t.rw.Read(p)
}

func (t *testTransport) Write(p []byte) (int, error) {
	if t.rw == nil {
		return 0, io.ErrClosedPipe
	}
	return t.rw.Write(p)
}
