package mqtt_test

import (
	"testing"

	mqtt "github.com/soypat/natiu-mqtt"
)

func TestHeaderLoopback(t *testing.T) {
	pubQoS0flag, err := mqtt.NewPublishFlags(mqtt.QoS0, false, true)
	if err != nil {
		t.Fatal(err)
	}
	for _, header := range []struct {
		tp    mqtt.PacketType
		flags mqtt.PacketFlags

		remlen uint32
	}{
		{tp: mqtt.PacketPubrel},
		{tp: mqtt.PacketPingreq},
		{tp: mqtt.PacketPublish, flags: pubQoS0flag},
		{tp: mqtt.PacketConnect},
	} {
		h, err := mqtt.NewHeader(header.tp, header.flags, header.remlen)
		if err != nil {
			t.Fatal(err)
		}
		if h.RemainingLength != header.remlen {
			t.Error("remaining length mismatch")
		}
		flagsGot := h.Flags()
		if header.tp == mqtt.PacketPublish && flagsGot != header.flags {
			t.Error("publish flag mismatch", flagsGot, header.flags)
		}
		typeGot := h.Type()
		if typeGot != header.tp {
			t.Error("type mismatch")
		}
	}
}
