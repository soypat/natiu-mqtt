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
	client := mqtt.NewClient(mqtt.DecoderNoAlloc{make([]byte, 1500)}, nil)

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
	varConn.SetDefaultMQTT([]byte("salamanca")) // Client automatically sets ClientID so no need to set here.
	err = client.StartConnect(conn, &varConn)   // Connect to server.
	if err != nil {
		// Error or loop until connect success.
		log.Fatalf("CONNECT failed: %v\n", err)
	}

	// Loop until connected to server or timeout.
	for i := 0; i < 4; i++ {
		time.Sleep(time.Second)
		if client.IsConnected() {
			break
		}
	}

	// Ping forever until error.
	for {
		pingErr := client.StartPing()
		if pingErr != nil {
			log.Fatal("ping error: ", pingErr, " with disconnect reason:", client.Err())
		}
		time.Sleep(time.Second)
		if client.AwaitingPingresp() {
			log.Println("Ping response took longer than a second")
		} else {
			log.Println("Ping success")
		}
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
