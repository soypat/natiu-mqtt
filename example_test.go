package mqtt_test

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"time"

	mqtt "github.com/soypat/natiu-mqtt"
)

func ExampleClient() {
	// Create new client.
	client := mqtt.NewClient(mqtt.ClientConfig{
		Decoder: mqtt.DecoderNoAlloc{make([]byte, 1500)},
		OnPub: func(_ mqtt.Header, _ mqtt.VariablesPublish, r io.Reader) error {
			message, _ := io.ReadAll(r)
			log.Println("received message:", string(message))
			return nil
		},
	})

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
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	err = client.Connect(ctx, conn, &varConn) // Connect to server.
	cancel()
	if err != nil {
		// Error or loop until connect success.
		log.Fatalf("connect attempt failed: %v\n", err)
	}

	// Ping forever until error.
	for {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		pingErr := client.Ping(ctx)
		cancel()
		if pingErr != nil {
			log.Fatal("ping error: ", pingErr, " with disconnect reason:", client.Err())
		}
		log.Println("ping success!")
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
