package mqtt

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"testing"
)

// Typical packets
var (
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

func TestVariablesConnectFlags(t *testing.T) {
	getFlags := func(flag byte) (username, password, willRetain, willFlag, cleanSession, reserved bool, qos QoSLevel) {
		return flag&(1<<7) != 0, flag&(1<<6) != 0, flag&(1<<5) != 0, flag&(1<<2) != 0, flag&(1<<1) != 0, flag&1 != 0, QoSLevel(flag>>3) & 0b11
	}
	var connect VariablesConnect
	connect.SetDefaultMQTT([]byte("salamanca"))
	flags := connect.Flags()
	usr, pwd, wR, wF, cs, forbidden, qos := getFlags(flags)
	if qos != QoS0 {
		t.Error("QoS0 default, got ", qos.String())
	}
	if usr || pwd {
		t.Error("expected no password or user on default flags")
	}
	if wR {
		t.Error("will retain set")
	}
	if wF {
		t.Error("will flag set")
	}
	if cs {
		t.Error("clean session set")
	}
	if forbidden {
		t.Error("forbidden bit set")
	}
	if defaultProtocolLevel != connect.ProtocolLevel {
		t.Error("protocol level mismatch")
	}
	if defaultProtocol != string(connect.Protocol) {
		t.Error("protocol mismatch")
	}
	connect.WillQoS = QoS2
	connect.Username = []byte("inigo")
	connect.Password = []byte("123")
	connect.CleanSession = true
	usr, pwd, wR, wF, cs, forbidden, qos = getFlags(connect.Flags())
	if qos != QoS2 {
		t.Error("QoS0 default, got ", qos.String())
	}
	if !usr {
		t.Error("username flag not ok")
	}
	if !pwd {
		t.Error("password flag not ok")
	}
	if wR {
		t.Error("will retain set")
	}
	if wF {
		t.Error("will flag set")
	}
	if !cs {
		t.Error("clean session not set")
	}
	if forbidden {
		t.Error("forbidden bit set")
	}
}

func TestVariablesConnectSize(t *testing.T) {
	var varConn VariablesConnect
	varConn.SetDefaultMQTT([]byte("salamanca"))
	varConn.WillQoS = QoS1
	varConn.WillRetain = true
	varConn.WillMessage = []byte("Hello, my name is Inigo Montoya. You killed my father. Prepare to die.")
	varConn.WillTopic = []byte("great-movies")
	varConn.Username = []byte("Inigo")
	varConn.Password = []byte("\x00\x01\x02\x03flab\xff\x7f\xff")
	got := varConn.Size()
	expect, err := encodeConnect(io.Discard, &varConn)
	if err != nil {
		t.Fatal(err)
	}
	if got != expect {
		t.Errorf("Size returned %d. encoding CONNECT variable header yielded %d", got, expect)
	}
}

func TestRxTxLoopback(t *testing.T) {
	// This test starts with a long running
	buf := newLoopbackTransport()
	rxtx, err := NewRxTx(buf, DecoderLowmem{make([]byte, 1500)})
	if err != nil {
		t.Fatal(err)
	}

	//
	// Send CONNECT packet over wire.
	//
	{
		var varConn VariablesConnect
		varConn.SetDefaultMQTT([]byte("0w"))
		varConn.WillQoS = QoS1
		varConn.WillRetain = true
		varConn.WillMessage = []byte("Aw")
		varConn.WillTopic = []byte("Bw")
		varConn.Username = []byte("Cw")
		varConn.Password = []byte("Dw")
		remlen := uint32(varConn.Size())
		expectHeader := newHeader(PacketConnect, 0, remlen)
		err = rxtx.WriteConnect(&varConn)
		if err != nil {
			t.Fatal(err)
		}
		// We now prepare to receive CONNECT packet on other side.
		callbackExecuted := false
		rxtx.OnConnect = func(rt *RxTx, vc *VariablesConnect) error {
			if rt.LastReceivedHeader != expectHeader {
				t.Errorf("rxtx header mismatch, expect:%v, rxed:%v", expectHeader.String(), rt.LastReceivedHeader.String())
			}
			varEqual(t, &varConn, vc)
			callbackExecuted = true
			return nil
		}
		// Read packet that is on the "wire"
		n, err := rxtx.ReadNextPacket()
		if err != nil {
			t.Fatal(err)
		}
		expectSize := expectHeader.Size() + varConn.Size()
		if n != expectSize {
			t.Errorf("read %v bytes, expected to read %v bytes", n, expectSize)
		}
		if !callbackExecuted {
			t.Error("OnConnect callback not executed")
		}
	}
	if t.Failed() {
		return // fix first clause before continuing.
	}
	//
	// Send CONNACK packet over wire.
	//
	{
		varConnck := VariablesConnack{
			AckFlags:   1,                            //SP set
			ReturnCode: ReturnCodeBadUserCredentials, // Bad User credentials
		}
		err = rxtx.WriteConnack(varConnck)
		if err != nil {
			t.Fatal(err)
		}
		expectHeader := newHeader(PacketConnack, 0, uint32(varConnck.Size()))
		callbackExecuted := false
		rxtx.OnConnack = func(rt *RxTx, vc VariablesConnack) error {
			if rt.LastReceivedHeader != expectHeader {
				t.Errorf("rxtx header mismatch, expect:%v, rxed:%v", expectHeader.String(), rt.LastReceivedHeader.String())
			}
			varEqual(t, varConnck, vc)
			callbackExecuted = true
			return nil
		}

		n, err := rxtx.ReadNextPacket()
		if err != nil {
			t.Fatal(err)
		}
		expectSize := expectHeader.Size() + varConnck.Size()
		if n != expectSize {
			t.Errorf("read %v bytes, expected to read %v bytes", n, expectSize)
		}
		if !callbackExecuted {
			t.Error("OnConnack callback not executed")
		}
	}

	//
	// Send PUBLISH packet over wire.
	//
	{
		publishPayload := []byte("ertytgbhjjhundsaip;vf[oniw[aondmiksfvoWDNFOEWOPndsafr;poulikujyhtgbfrvdcsxzaesxt dfcgvfhbg kjnlkm/'.")
		varPublish := VariablesPublish{
			TopicName:        []byte("now-for-something-completely-different"),
			PacketIdentifier: math.MaxUint16,
		}
		pubflags, err := NewPublishFlags(QoS1, true, true)
		if err != nil {
			t.Fatal(err)
		}

		publishHeader := newHeader(PacketPublish, pubflags, uint32(varPublish.Size()+len(publishPayload)))
		err = rxtx.WritePublishPayload(publishHeader, varPublish, publishPayload)
		if err != nil {
			t.Fatal(err)
		}
		callbackExecuted := false
		rxtx.OnPub = func(rt *RxTx, vp VariablesPublish, r io.Reader) error {
			b, err := io.ReadAll(r)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(b, publishPayload) {
				t.Error("got different payloads!")
			}
			if rt.LastReceivedHeader != publishHeader {
				t.Errorf("rxtx header mismatch, txed:%v, rxed:%v", publishHeader.String(), rt.LastReceivedHeader.String())
			}
			varEqual(t, varPublish, vp)
			callbackExecuted = true
			return nil
		}

		n, err := rxtx.ReadNextPacket()
		if err != nil {
			t.Fatal(err)
		}
		expectSize := publishHeader.Size() + varPublish.Size()
		if n != expectSize {
			t.Errorf("read %v bytes, expected to read %v bytes", n, expectSize)
		}
		if !callbackExecuted {
			t.Error("OnPub callback not executed")
		}
	}

	//
	// Send SUBSCRIBE packet over wire.
	//
	{

		varsub := VariablesSubscribe{
			PacketIdentifier: math.MaxUint16,
			TopicFilters: []SubscribeRequest{
				{TopicFilter: []byte("favorites"), QoS: QoS2},
				{TopicFilter: []byte("the-clash"), QoS: QoS2},
				{TopicFilter: []byte("always-watching"), QoS: QoS2},
				{TopicFilter: []byte("k-pop"), QoS: QoS2},
			},
		}
		err = rxtx.WriteSubscribe(varsub)
		if err != nil {
			t.Fatal(err)
		}

		expectHeader := newHeader(PacketSubscribe, PacketFlagsPubrelSubUnsub, uint32(varsub.Size()))
		callbackExecuted := false
		rxtx.OnSub = func(rt *RxTx, vs VariablesSubscribe) error {
			if rt.LastReceivedHeader != expectHeader {
				t.Errorf("rxtx header mismatch, expect:%v, rxed:%v", expectHeader.String(), rt.LastReceivedHeader.String())
			}
			varEqual(t, varsub, vs)
			callbackExecuted = true
			return nil
		}

		n, err := rxtx.ReadNextPacket()
		if err != nil {
			t.Fatal(err)
		}
		expectSize := expectHeader.Size() + varsub.Size()
		if n != expectSize {
			t.Errorf("read %v bytes, expected to read %v bytes", n, expectSize)
		}
		if !callbackExecuted {
			t.Error("OnSub callback not executed")
		}
	}

	//
	// Send UNSUBSCRIBE packet over wire.
	//
	{
		callbackExecuted := false
		varunsub := VariablesUnsubscribe{
			PacketIdentifier: math.MaxUint16,
			Topics:           bytes.Fields([]byte("topic1 topic2 topic3 semperfi")),
		}
		err = rxtx.WriteUnsubscribe(varunsub)
		if err != nil {
			t.Fatal(err)
		}
		expectHeader := newHeader(PacketUnsubscribe, PacketFlagsPubrelSubUnsub, uint32(varunsub.Size()))
		rxtx.OnUnsub = func(rt *RxTx, vu VariablesUnsubscribe) error {
			if rt.LastReceivedHeader != expectHeader {
				t.Errorf("rxtx header mismatch, expect:%v, rxed:%v", expectHeader.String(), rt.LastReceivedHeader.String())
			}
			varEqual(t, varunsub, vu)
			callbackExecuted = true
			return nil
		}

		n, err := rxtx.ReadNextPacket()
		if err != nil {
			t.Fatal(err)
		}
		expectSize := expectHeader.Size() + varunsub.Size()
		if n != expectSize {
			t.Errorf("read %v bytes, expected to read %v bytes", n, expectSize)
		}
		if !callbackExecuted {
			t.Error("OnUnsub callback not executed")
		}
	}

	//
	// Send SUBACK packet over wire.
	//
	{
		callbackExecuted := false
		varSuback := VariablesSuback{
			PacketIdentifier: math.MaxUint16,
			ReturnCodes:      []QoSLevel{QoS0, QoS1, QoS0, QoS2, QoSSubfail, QoS1},
		}
		err = rxtx.WriteSuback(varSuback)
		if err != nil {
			t.Fatal(err)
		}
		expectHeader := newHeader(PacketSuback, 0, uint32(varSuback.Size()))
		rxtx.OnSuback = func(rt *RxTx, vu VariablesSuback) error {
			if rt.LastReceivedHeader != expectHeader {
				t.Errorf("rxtx header mismatch, expect:%v, rxed:%v", expectHeader.String(), rt.LastReceivedHeader.String())
			}
			varEqual(t, varSuback, vu)
			callbackExecuted = true
			return nil
		}

		n, err := rxtx.ReadNextPacket()
		if err != nil {
			t.Fatal(err)
		}
		expectSize := expectHeader.Size() + varSuback.Size()
		if n != expectSize {
			t.Errorf("read %v bytes, expected to read %v bytes", n, expectSize)
		}
		if !callbackExecuted {
			t.Error("OnSuback callback not executed")
		}
	}

	//
	// Send PUBREL packet over wire.
	//
	{
		callbackExecuted := false
		txPI := uint16(3232)
		txHeader := newHeader(PacketPubrel, PacketFlagsPubrelSubUnsub, 2)
		err = rxtx.WriteOther(txHeader, txPI)
		if err != nil {
			t.Fatal(err)
		}

		rxtx.OnOther = func(rt *RxTx, gotPI uint16) error {
			if rt.LastReceivedHeader != txHeader {
				t.Errorf("rxtx header mismatch, expect:%v, rxed:%v", txHeader.String(), rt.LastReceivedHeader.String())
			}
			if gotPI != txPI {
				t.Error("mismatch of packet identifiers", gotPI, txPI)
			}
			callbackExecuted = true
			return nil
		}

		n, err := rxtx.ReadNextPacket()
		if err != nil {
			t.Fatal(err)
		}
		expectSize := txHeader.Size() + 2
		if n != expectSize {
			t.Errorf("read %v bytes, expected to read %v bytes", n, expectSize)
		}
		if !callbackExecuted {
			t.Error("OnOther callback not executed")
		}
	}
}

func newLoopbackTransport() io.ReadWriteCloser {
	var _buf bytes.Buffer
	// buf := bufio.NewReadWriter(bufio.NewReader(&_buf), bufio.NewWriter(&_buf))
	return &testTransport{&_buf}
}

type testTransport struct {
	rw io.ReadWriter
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

// varEqual errors test if a's fields not equal to b. Takes as argument all VariablesPACKET structs.
// Expects pointer to VariablesConnect.
func varEqual(t *testing.T, a, b any) {
	switch va := a.(type) {
	case *VariablesConnect:
		// Make name distinct to va to catch bugs easier.
		veebee := b.(*VariablesConnect)
		if va.CleanSession != veebee.CleanSession {
			t.Error("clean session mismatch")
		}
		if va.ProtocolLevel != veebee.ProtocolLevel {
			t.Error("protocol level mismatch")
		}
		if va.KeepAlive != veebee.KeepAlive {
			t.Error("willQoS mismatch")
		}
		if va.WillQoS != veebee.WillQoS {
			t.Error("willQoS mismatch")
		}
		if !bytes.Equal(va.ClientID, veebee.ClientID) {
			t.Error("client id mismatch")
		}
		if !bytes.Equal(va.Protocol, veebee.Protocol) {
			t.Error("protocol mismatch")
		}
		if !bytes.Equal(va.Password, veebee.Password) {
			t.Error("password mismatch")
		}
		if !bytes.Equal(va.Username, veebee.Username) {
			t.Error("username mismatch")
		}
		if !bytes.Equal(va.WillMessage, veebee.WillMessage) {
			t.Error("will message mismatch")
		}
		if !bytes.Equal(va.WillTopic, veebee.WillTopic) {
			t.Error("will topic mismatch")
		}

	case VariablesConnack:
		vb := b.(VariablesConnack)
		if va != vb {
			t.Error("CONNACK not equal:", va, vb)
		}

	case VariablesPublish:
		vb := b.(VariablesPublish)
		if !bytes.Equal(va.TopicName, vb.TopicName) {
			t.Error("publish topic names mismatch")
		}
		if va.PacketIdentifier != vb.PacketIdentifier {
			t.Error("packet id mismatch")
		}

	case VariablesSuback:
		vb := b.(VariablesSuback)
		if va.PacketIdentifier != vb.PacketIdentifier {
			t.Error("SUBACK packet identifier mismatch")
		}
		for i, rca := range va.ReturnCodes {
			rcb := vb.ReturnCodes[i]
			if rca != rcb {
				t.Errorf("SUBACK %dth return code mismatch, %s! = %s", i, rca, rcb)
			}
		}

	case VariablesSubscribe:
		vb := b.(VariablesSubscribe)
		if va.PacketIdentifier != vb.PacketIdentifier {
			t.Error("SUBSCRIBE packet identifier mismatch")
		}
		for i, hotopicA := range va.TopicFilters {
			hotTopicB := vb.TopicFilters[i]
			if hotopicA.QoS != hotTopicB.QoS {
				t.Errorf("SUBSCRIBE %dth QoS mismatch, %s! = %s", i, hotopicA.QoS, hotTopicB.QoS)
			}
			if !bytes.Equal(hotopicA.TopicFilter, hotTopicB.TopicFilter) {
				t.Errorf("SUBSCRIBE %dth topic filter mismatch, %s! = %s", i, string(hotopicA.TopicFilter), string(hotTopicB.TopicFilter))
			}
		}

	case VariablesUnsubscribe:
		vb := b.(VariablesUnsubscribe)
		if va.PacketIdentifier != vb.PacketIdentifier {
			t.Error("UNSUBSCRIBE packet identifier mismatch", va.PacketIdentifier, vb.PacketIdentifier)
		}
		for i, coldtopicA := range va.Topics {
			coldTopicB := vb.Topics[i]
			if !bytes.Equal(coldtopicA, coldTopicB) {
				t.Errorf("UNSUBSCRIBE %dth topic mismatch, %s! = %s", i, coldtopicA, coldTopicB)
			}
		}

	default:
		panic(fmt.Sprintf("%T undefined in varEqual", va))
	}
}
