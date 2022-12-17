package mqtt_test

import (
	"fmt"
	"log"
	"net"
	"time"

	mqtt "github.com/soypat/natiu-mqtt"
)

func ExampleClient() {
	// Create new client.
	client := mqtt.NewClient(mqtt.DecoderNoAlloc{make([]byte, 1500)})
	client.ID = "salamanca"

	// Get a transport for MQTT packets.
	const defaultMQTTPort = ":1883"
	conn, err := net.Dial("tcp", "127.0.0.1"+defaultMQTTPort)
	if err != nil {
		fmt.Println(err)
		return
	}
	client.SetTransport(conn)
	// Prepare for CONNECT interaction with server.
	var varConn mqtt.VariablesConnect
	varConn.SetDefaultMQTT(nil)              // Client automatically sets ClientID so no need to set here.
	connack, err := client.Connect(&varConn) // Connect to server.
	if err != nil {
		// Error or loop until connect success.
		log.Fatalf("CONNECT failed with return code %d: %v\n", connack.ReturnCode, err)
	}
	// Ping forever until error.
	var pingErr error
	for pingErr = client.Ping(); pingErr == nil; pingErr = client.Ping() {
		log.Println("Ping success")
		time.Sleep(time.Second)
	}
	// Output:
	// dial tcp 127.0.0.1:1883: connect: connection refused
}

func ExampleRxTx() {
	const defaultMQTTPort = ":1883"
	conn, err := net.Dial("tcp", "127.0.0.1"+defaultMQTTPort)
	if err != nil {
		log.Fatal(err)
	}
	rxtx, err := mqtt.NewRxTx(conn, mqtt.DecoderNoAlloc{UserBuffer: make([]byte, 1500)})
	if err != nil {
		log.Fatal(err)
	}
	rxtx.RxCallbacks.OnConnack = func(rt *mqtt.Rx, vc mqtt.VariablesConnack) error {
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
}
