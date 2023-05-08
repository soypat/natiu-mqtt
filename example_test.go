package mqtt_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"time"

	mqtt "github.com/soypat/natiu-mqtt"
)

func ExampleClient_concurrent() {
	// Create new client.
	received := make(chan []byte, 10)
	client := mqtt.NewClient(mqtt.ClientConfig{
		Decoder: mqtt.DecoderNoAlloc{make([]byte, 1500)},
		OnPub: func(_ mqtt.Header, _ mqtt.VariablesPublish, r io.Reader) error {
			message, _ := io.ReadAll(r)
			if len(message) > 0 {
				select {
				case received <- message:
				default:
					// If channel is full we ignore message.
				}
			}
			log.Println("received message:", string(message))
			return nil
		},
	})

	// Set the connection parameters and set the Client ID to "salamanca".
	var varConn mqtt.VariablesConnect
	varConn.SetDefaultMQTT([]byte("salamanca"))

	// Define an inline function that connects the MQTT client automatically.
	tryConnect := func() error {
		// Get a transport for MQTT packets using the local host and default MQTT port (1883).
		conn, err := net.Dial("tcp", "127.0.0.1:1883")
		if err != nil {
			return err
		}
		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
		defer cancel()
		return client.Connect(ctx, conn, &varConn) // Connect to server.
	}
	// Attempt first connection and fail immediately if that does not work.
	err := tryConnect()
	if err != nil {
		log.Println(err)
		return
	}

	// Call read goroutine. Read goroutine will also handle reconnection
	// when client disconnects.
	go func() {
		for {
			if !client.IsConnected() {
				time.Sleep(time.Second)
				tryConnect()
				continue
			}
			err = client.HandleNext()
			if err != nil {
				log.Println("HandleNext failed:", err)
			}
		}
	}()

	// Call Write goroutine and create a channel to serialize messages
	// that we want to send out.
	const TOPICNAME = "/mqttnerds"
	pubFlags, _ := mqtt.NewPublishFlags(mqtt.QoS0, false, false)
	varPub := mqtt.VariablesPublish{
		TopicName: []byte(TOPICNAME),
	}
	txQueue := make(chan []byte, 10)
	go func() {
		for {
			if !client.IsConnected() {
				time.Sleep(time.Second)
				continue
			}
			message := <-txQueue
			varPub.PacketIdentifier = uint16(rand.Int())
			// Loop until message is sent succesfully. This guarantees
			// all messages are sent, even in events of disconnect.
			for {
				err := client.PublishPayload(pubFlags, varPub, message)
				if err == nil {
					break
				}
				time.Sleep(time.Second)
			}
		}
	}()

	// Main program logic.
	for {
		message := <-received
		// We transform the message and send it back out.
		fields := bytes.Fields(message)
		message = bytes.Join(fields, []byte(","))
		txQueue <- message
	}
}

func ExampleClient() {
	// Create new client with default settings.
	client := mqtt.NewClient(mqtt.ClientConfig{})

	// Get a transport for MQTT packets.
	const defaultMQTTPort = ":1883"
	conn, err := net.Dial("tcp", "test.mosquitto.org"+defaultMQTTPort)
	if err != nil {
		fmt.Println(err)
		return
	}

	// Prepare for CONNECT interaction with server.
	var varConn mqtt.VariablesConnect
	varConn.SetDefaultMQTT([]byte("salamanca"))
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	err = client.Connect(ctx, conn, &varConn) // Connect to server.
	cancel()
	if err != nil {
		// Error or loop until connect success.
		log.Fatalf("connect attempt failed: %v\n", err)
	}
	fmt.Println("connection success")

	defer func() {
		err := client.Disconnect(errors.New("end of test"))
		if err != nil {
			fmt.Println("disconnect failed:", err)
		}
	}()

	// Ping forever until error.
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	pingErr := client.Ping(ctx)
	cancel()
	if pingErr != nil {
		log.Fatal("ping error: ", pingErr, " with disconnect reason:", client.Err())
	}
	fmt.Println("ping success!")
	// Output:
	// connection success
	// ping success!
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
