[![Go Report Card](https://goreportcard.com/badge/github.com/soypat/natiu-mqtt)](https://goreportcard.com/report/github.com/soypat/natiu-mqtt)
[![GoDoc](https://godoc.org/github.com/soypat/natiu-mqtt?status.svg)](https://godoc.org/github.com/soypat/natiu-mqtt)
[![codecov](https://codecov.io/gh/soypat/natiu-mqtt/branch/main/graph/badge.svg)](https://codecov.io/gh/soypat/natiu-mqtt/branch/main)

# natiu-mqtt
### A dead-simple, extensible and correct MQTT implementation.

**Natiu**: Means *mosquito* in the [Guaraní language](https://en.wikipedia.org/wiki/Guarani_language), a language spoken primarily in Paraguay. Commonly written as ñati'û or ñati'ũ.

## Highlights
* **Modular**
    * Client implementation leaves allocating parts up to the [`Decoder`](./mqtt.go) interface type. Users can choose to use non-allocating or allocating implementations of the 3 method interface.
    * [`RxTx`](./rxtx.go) type lets one build an MQTT implementation from scratch for any transport. No server/client logic defined at this level.

* **No uneeded allocations**: The PUBLISH application message is not handled by this library, the user receives an `io.Reader` with the underlying transport bytes. This prevents allocations on `natiu-mqtt` side.
* **V3.1.1**: Compliant with [MQTT version 3.1.1](http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html) for QoS0 interactions. _QoS1 and QoS2 are WIP._
* **No external dependencies**: Nada. Nope.
* **Data oriented design**: Minimizes abstractions or objects for the data on the wire.
* **Fuzz tested, robust**: Decoding implementation fuzzed to prevent adversarial user input from crashing application (95% coverage).
* **Simplicity**: A simple base package yields simple implementations for different transports. See [Implementations section](#implementations).
* **Runtime-what?**: Unlike other MQTT implementations. **No** channels, **no** interface conversions, **no** goroutines- as little runtimey stuff as possible. You get the best of Go's concrete types when using Natiu's API. Why? Because MQTT deserialization and serialization are an *embarrassingly* serial and concrete problem.

## Goals
This implementation will have a simple embedded-systems implementation in the package
top level. This implementation will be transport agnostic and non-concurrent. This will make it far easier to modify and reason about. The transport dependent implementations will have their own subpackage, so one package for TCP transport, another for UART, PPP etc.

* Minimal, if any, heap allocations.
* Support for TCP transport.
* User owns payload bytes.

## Implementations
- [natiu-wsocket](https://github.com/soypat/natiu-wsocket): MQTT via **Websockets**. Tested with [moscajs/aedes broker server.](https://github.com/moscajs/aedes).

## Examples
API subject to before v1.0.0 release.

### Example use of `Client`

```go
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
```

## Why not just use paho?

Some issues with Eclipse's Paho implementation:
* [Inherent data races on API side](https://github.com/eclipse/paho.mqtt.golang/issues/550). The implementation is so notoriously hard to modify this issue has been in a frozen state.
* Calling Client.Disconnect when client is already disconnected blocks indefinetely and can cause deadlock or spin with Paho's implementation. 
* If there is an issue with the network and Reconnect is enabled then then Paho's Reconnect spins. There is no way to prevent this.
* Interfaces used for ALL data types. This is not necessary and makes it difficult to work with since there is no in-IDE documentation on interface methods.
* No lower level abstraction of MQTT for use in embedded systems with non-TCP transport.
* Uses `any` interface for the payload, which could simply be a byte slice...

I found these issues after a 2 hour dev session. There will undoubtedly be more if I were to try to actually get it working...
