package mqtt_test

import (
	"log"
	"net"
	"testing"

	mqtt "github.com/soypat/natiu-mqtt"
)

func ExampleRxTx() {
	const defaultMQTTPort = ":1883"
	conn, err := net.Dial("tcp", "127.0.0.1"+defaultMQTTPort)
	if err != nil {
		log.Fatal(err)
	}
	rxtx, err := mqtt.NewRxTx(conn, mqtt.DecoderLowmem{UserBuffer: make([]byte, 1500)})
	if err != nil {
		log.Fatal(err)
	}
	rxtx.OnConnack = func(rt *mqtt.Rx, vc mqtt.VariablesConnack) error {
		log.Printf("%v received, SP=%v, rc=%v", rt.LastReceivedHeader.String(), vc.SessionPresent(), vc.ReturnCode.String())
		return nil
	}
	// PacketFlags set automatically for all packets that are not PUBLISH. So set to 0.
	varConnect := mqtt.VariablesConnect{
		ClientID:      []byte("salamanca"),
		Protocol:      []byte("MQTT"),
		ProtocolLevel: 4,
		KeepAlive:     60,
		CleanSession:  true,
		WillMessage:   []byte("MQTT is okay, I guess"),
		WillTopic:     []byte("mqttnerds"),
		WillRetain:    true,
	}
	err = rxtx.WriteConnect(&varConnect)
	if err != nil {
		log.Fatal(err)
	}
	// TODO(soypat): Build a server-client example.
	// Output:
	// 2022/12/12 18:07:18 dial tcp 127.0.0.1:1883: connect: connection refused
}

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
