# natiu-mqtt
### A dead-simple, extensible and correct MQTT implementation.

**Natiu**: Means *mosquito* in the [Guaraní language](https://en.wikipedia.org/wiki/Guarani_language), a language spoken primarily in Paraguay. Commonly written as ñati'û or ñati'ũ.

_Still a WIP._

## Highlights
* **Modular**
    * Client implementation leaves allocating parts up to the [`Decoder`](./mqtt.go) interface type. Users can choose to use non-allocating or allocating implementations of the 3 method interface.
    * [`RxTx`](./rxtx.go) type lets one build an MQTT implementation from scratch for any transport. No server/client logic defined at this level.

* **No uneeded allocations**: The PUBLISH application message is not handled by this library, the user receives an `io.Reader` with the underlying transport bytes. This prevents allocations on `natiu-mqtt` side.
* **V3.1.1**: Compliant with [MQTT version 3 rev 1 (3.1)](https://public.dhe.ibm.com/software/dw/webservices/ws-mqtt/mqtt-v3r1.html)
* **No external dependencies**: Nada. Nope.
* **Data oriented design**: Minimizes abstractions or objects for the data on the wire.


## Goals
This implementation will have a simple embedded-systems implementation in the package
top level. This implementation will be transport agnostic and non-concurrent. This will make it far easier to modify and reason about. The transport dependent implementations will have their own subpackage, so one package for TCP transport, another for UART, PPP etc.

* Minimal, if any, heap allocations (only using `unsafe` or `tinygo` build tags).
* Support for TCP transport.
* User owns payload bytes.

### Example of using a TCP connection with an RxTx
API subject to change.
```go
func main() {
    // Dial a TCP connection.
    const defaultMQTTPort = ":1883"
	conn, err := net.Dial("tcp", "127.0.0.1"+defaultMQTTPort)
	if err != nil {
		log.Fatal(err)
	}

    // Create the RxTx MQTT IO handler.
	rxtx, err := mqtt.NewRxTx(conn, mqtt.DecoderLowmem{UserBuffer: make([]byte, 1500)})
	if err != nil {
		log.Fatal(err)
	}
    // Add a handler on CONNACK packet.
	rxtx.OnConnack = func(rt *mqtt.RxTx, vc mqtt.VariablesConnack) error {
		log.Printf("%v received, SP=%v, rc=%v", rt.LastReceivedHeader.String(), vc.SessionPresent(), vc.ReturnCode.String())
		return nil
	}
	
    // Prepare to send first MQTT packet over wire.
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
    // Header set automatically for all packets that are not PUBLISH.
	err = rxtx.WriteConnect(&varConnect)
	if err != nil {
		log.Fatal(err)
	}
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