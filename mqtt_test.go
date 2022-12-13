package mqtt

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"testing"
)

func FuzzRxTxReadNextPacket(f *testing.F) {
	const maxSize = 1500
	testCases := [][]byte{
		// Typical connect packet.
		[]byte("\x10\x1e\x00\x04MQTT\x04\xec\x00<\x00\x020w\x00\x02Bw\x00\x02Aw\x00\x02Cw\x00\x02Dw"),
		// Typical connack
		[]byte("\x02\x01\x04"),
		// A Publish packet
		[]byte(";\x8e\x01\x00&now-for-something-completely-different\xff\xffertytgbhjjhundsaip;vf[oniw[aondmiksfvoWDNFOEWOPndsafr;poulikujyhtgbfrvdcsxzaesxt dfcgvfhbg kjnlkm/'."),
		// A subscribe packet.
		[]byte("\x824\xff\xff\x00\tfavorites\x02\x00\tthe-clash\x02\x00\x0falways-watching\x02\x00\x05k-pop\x02"),
		// Unsubscribe packet
		[]byte("\xa2$\xff\xff\x00\x06topic1\x00\x06topic2\x00\x06topic3\x00\bsemperfi"),
		// Suback packet.
		[]byte("\x90\b\xff\xff\x00\x01\x00\x02\x80\x01"),
		// Pubrel packet.
		[]byte("b\x02\f\xa0"),
	}
	testCases = append(testCases, fuzzCorpus...)
	for _, tc := range testCases {
		f.Add(tc) // Provide seed corpus.
	}

	f.Fuzz(func(t *testing.T, a []byte) {
		if len(a) == 0 || len(a) > maxSize {
			return
		}
		buf := newLoopbackTransport()
		_, err := writeFull(buf, a)
		if err != nil {
			t.Fatal(err)
		}
		rxtx, err := NewRxTx(buf, DecoderLowmem{make([]byte, maxSize+10)})
		if err != nil {
			t.Fatal(err)
		}
		rxtx.ReadNextPacket()
	})
	_ = testCases
}

func TestRxTxBadPacketRxErrors(t *testing.T) {
	rxtx, err := NewRxTx(&testTransport{}, DecoderLowmem{UserBuffer: make([]byte, 1500)})
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		reason string
		rx     []byte
	}{
		{"no contents", []byte("")},
		{"EOF during fixed header", []byte("\x01")},
		{"forbidden packet type 0", []byte("\x00\x00")},
		{"forbidden packet type 15", []byte("\xf0\x00")},
		{"missing CONNECT var header and bad remaining length", []byte("\x10\x0a")},
		{"missing CONNECT var header", []byte("\x10\x00")},
		{"missing CONNACK var header", []byte("\x20\x00")},
		{"missing PUBLISH var header", []byte("\x30\x00")},
		{"missing PUBACK var header", []byte("\x40\x00")},
		{"missing SUBSCRIBE var header", []byte("\x80\x00")},
		{"missing SUBACK var header", []byte("\x90\x00")},
		{"missing UNSUBSCRIBE var header", []byte("\xa0\x00")},
		{"missing UNSUBACK var header", []byte("\xb0\x00")},
	} {
		buf := newLoopbackTransport()
		rxtx.SetTransport(buf)
		n, err := buf.Write(test.rx)
		if err != nil || n != len(test.rx) {
			t.Fatal("all bytes not written or error:", err)
		}
		_, err = rxtx.ReadNextPacket()
		if err == nil {
			t.Error("expected error for case:", test.reason)
		}
	}
}

func TestHasPacketIdentifer(t *testing.T) {
	const (
		qos0Flag = PacketFlags(QoS0 << 1)
		qos1Flag = PacketFlags(QoS1 << 1)
		qos2Flag = PacketFlags(QoS2 << 1)
	)
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
		t.Log("tested ", test.h.String())
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

func TestHeaderSize(t *testing.T) {
	for _, test := range []struct {
		h      Header
		expect int
	}{
		{h: newHeader(1, 0, 0), expect: 2},
		{h: newHeader(1, 0, 1), expect: 2},
		{h: newHeader(1, 0, 2), expect: 2},
		{h: newHeader(1, 0, 128), expect: 3},
		{h: newHeader(1, 0, 0xffff), expect: 4},
		{h: newHeader(1, 0, 0xffff_ff), expect: 5},
		{h: newHeader(1, 0, 0xffff_ffff), expect: 0}, // bad remaining length
	} {
		got := test.h.Size()
		if got != test.expect {
			t.Error("size mismatch for remlen:", test.h.RemainingLength, got, test.expect)
		}
	}
}

func TestHeaderEncodeDecodeLoopback(t *testing.T) {
	var b bytes.Buffer
	for _, test := range []struct {
		h      Header
		expect int
	}{
		{h: newHeader(1, 0, maxRemainingLengthValue), expect: 5},   // TODO(soypat): must support up to maxRemainingLengthValue remaining length.
		{h: newHeader(1, 0, maxRemainingLengthValue+1), expect: 0}, // bad remaining length
		{h: newHeader(1, 0, 0), expect: 2},
		{h: newHeader(1, 0, 1), expect: 2},
		{h: newHeader(1, 0, 2), expect: 2},
		{h: newHeader(1, 0, 128), expect: 3},
		{h: newHeader(1, 0, 0xffff), expect: 4},
	} {
		hdr := test.h
		nencode, err := hdr.Encode(&b)
		if hdr.RemainingLength > maxRemainingLengthValue {
			if err == nil {
				t.Error("expected error for malformed packet")
			}
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		gotHdr, ndecode, err := DecodeHeader(&b)
		if err != nil {
			t.Fatalf("decoded %d byte for %+v: %v", ndecode, hdr, err)
		}
		if nencode != ndecode {
			t.Errorf("number of bytes encoded (%d) not match decoded (%d)", nencode, ndecode)
		}
		if nencode != test.expect {
			t.Errorf("expected to encode %d bytes, encoded %d: %s", test.expect, nencode, hdr)
		}
		if hdr != gotHdr {
			t.Errorf("header mismatch in values encode:%+v; decode:%+v", hdr, gotHdr)
		}
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
	rxtx.SetTransport(buf)
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
		rxtx.OnConnect = func(rt *Rx, vc *VariablesConnect) error {
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
	buf.rw.Reset()
	//
	// Send CONNACK packet over wire.
	//
	{
		varConnck := VariablesConnack{
			AckFlags:   1,                      // SP set
			ReturnCode: ReturnCodeConnAccepted, // Accepted since SP set.
		}
		t.Log("loopback connack:", varConnck.String())
		err = rxtx.WriteConnack(varConnck)
		if err != nil {
			t.Fatal(err)
		}
		expectHeader := newHeader(PacketConnack, 0, uint32(varConnck.Size()))
		callbackExecuted := false
		rxtx.OnConnack = func(rt *Rx, vc VariablesConnack) error {
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
		rxtx.OnPub = func(rt *Rx, vp VariablesPublish, r io.Reader) error {
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
	// Send PUBLISH packet over wire and ignore packet.
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
		rxtx.OnPub = nil

		n, err := rxtx.ReadNextPacket()
		if err != nil {
			t.Fatal(err)
		}
		expectSize := publishHeader.Size() + varPublish.Size()
		if n != expectSize {
			t.Errorf("read %v bytes, expected to read %v bytes", n, expectSize)
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
		rxtx.OnSub = func(rt *Rx, vs VariablesSubscribe) error {
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
		rxtx.OnUnsub = func(rt *Rx, vu VariablesUnsubscribe) error {
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
		rxtx.OnSuback = func(rt *Rx, vu VariablesSuback) error {
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
		expectHeader := newHeader(PacketPubrel, PacketFlagsPubrelSubUnsub, 2)
		err = rxtx.WriteIdentified(PacketPubrel, txPI)
		if err != nil {
			t.Fatal(err)
		}

		rxtx.OnOther = func(rt *Rx, gotPI uint16) error {
			if rt.LastReceivedHeader != expectHeader {
				t.Errorf("rxtx header mismatch, expect:%v, rxed:%v", expectHeader.String(), rt.LastReceivedHeader.String())
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
		expectSize := expectHeader.Size() + 2
		if n != expectSize {
			t.Errorf("read %v bytes, expected to read %v bytes", n, expectSize)
		}
		if !callbackExecuted {
			t.Error("OnOther callback not executed")
		}
	}

	//
	// Send PINGREQ packet over wire.
	//
	{
		callbackExecuted := false
		expectHeader := newHeader(PacketPingreq, 0, 0)
		err = rxtx.WriteSimple(PacketPingreq)
		if err != nil {
			t.Fatal(err)
		}

		rxtx.OnOther = func(rt *Rx, gotPI uint16) error {
			if rt.LastReceivedHeader != expectHeader {
				t.Errorf("rxtx header mismatch, expect:%v, rxed:%v", expectHeader.String(), rt.LastReceivedHeader.String())
			}
			if gotPI != 0 {
				t.Error("mismatch of packet identifiers", gotPI, 0)
			}
			callbackExecuted = true
			return nil
		}

		n, err := rxtx.ReadNextPacket()
		if err != nil {
			t.Fatal(err)
		}
		expectSize := expectHeader.Size()
		if n != expectSize {
			t.Errorf("read %v bytes, expected to read %v bytes", n, expectSize)
		}
		if !callbackExecuted {
			t.Error("OnOther callback not executed")
		}
	}
	err = rxtx.Close()
	if err != nil {
		t.Error(err)
	}
}

func newLoopbackTransport() *testTransport {
	var _buf bytes.Buffer
	// buf := bufio.NewReadWriter(bufio.NewReader(&_buf), bufio.NewWriter(&_buf))
	return &testTransport{&_buf}
}

type testTransport struct {
	rw *bytes.Buffer
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

var fuzzCorpus = [][]byte{
	[]byte("00\x0000"),
	[]byte("\x90\xa7000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"),
	[]byte("\xa2A00\x00\x06000000\x00\x06000000\x00\b00000000\x00\x06000000\x00\x06000000\x00\x06000000\x00\b000000000"),
	[]byte("\x900000000000000000000"),
	[]byte("\x100\x00\x0400000\xec00\x00\x0200\x00\x0200\x0000"),
	[]byte("\x82000"),
	[]byte("\x900000000000000000"),
	[]byte("\x90000000000000000000"),
	[]byte("\x100\x00\x0400000\x8000\x00\x0200\x0000"),
	[]byte("\x100\x00\xbf00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"),
	[]byte("\xa000"),
	[]byte("20\x0000"),
	[]byte("\x82000\x00\t0000000000\x00\x020000"),
	[]byte("\x100\x00\x0400000$00\x00\x0200\x0000"),
	[]byte("00\x0400000000000000000000000000000000000000000000000000000000000000"),
	[]byte("\x900000"),
	[]byte("\xa00"),
	[]byte("0"),
	[]byte(" 00"),
	[]byte("0\xfe\xff\xff"),
	[]byte("a0"),
	[]byte("A0"),
	[]byte("\x100\x0200"),
	[]byte("\x100\x00\x0400000"),
	[]byte("\x100\x00\x0400000$00\x0000"),
	[]byte("\x100\x00\x04000000"),
	[]byte("\xa2 00\x00000000000000000000000000000000000"),
	[]byte("\xa2000\x00\x06000000\x00\x060000000"),
	[]byte("00\x00\x000"),
	[]byte("\x100\x00\x0400000\xec00\x00\x0200\x00\x0200\x00\x0200\x00\x0200\x00"),
	[]byte("\x82000\x00\x0200000"),
	[]byte("\x820"),
	[]byte("\x9000000"),
	[]byte("0\xee\xff\xff"),
	[]byte("0\x8e\x01\x00 00000000000000000000000000000000000000000"),
	[]byte(" 0\x00"),
	[]byte("0\xff\xc4"),
	[]byte("\x90\xa700000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"),
	[]byte("\x9000000000000000000000000000000000000"),
	[]byte("\x82000\x000"),
	[]byte(" 0"),
	[]byte("\x100\x00\x0400000B"),
	[]byte("\x82000\x0000"),
	[]byte("\x90000"),
	[]byte("000"),
	[]byte("0\xff0\x00\t0000000000"),
	[]byte("\x100\x00\x040000000000"),
	[]byte("0\xbb0\x0100"),
	[]byte("\x9000000000000"),
	[]byte(""),
	[]byte("\x100\x00\x04000001"),
	[]byte("\x100\x00\x0400000$00\x05\xe20"),
	[]byte("b0"),
	[]byte("\xa2A00\x00\x06000000\x00\x06000000\x00\b00000000\x00\x06000000\x00\x06000000\x00\x06000000\x00\b00000000"),
	[]byte("\x90B0000000000000000000000000000000000000000000000000000000000000000000"),
	[]byte("\x100\x00\x0400000000\x0000"),
	[]byte("\x900"),
	[]byte("\x90\xa700000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"),
	[]byte("\xa2A00\x00\x06000000\x00\x06000000\x00\b00000000\x00\x06000000\x00\x06000000\x00\x06000000\x00\b00000000\x0000"),
	[]byte("00\x0400"),
	[]byte("\x100"),
}
