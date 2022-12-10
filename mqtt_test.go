package mqtt

import "testing"

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
		{h: newHeader(PacketConnack, 0, 0), expect: false},
		{h: newHeader(PacketConnect, 0, 0), expect: false},
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
