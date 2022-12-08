# natiu-mqtt
### A low-level, correct MQTT implementation.
A better-than-paho MQTT client implementation where the user is owner of payload memory making it suited
for low memory usage applications.

Using [MQTT version 3 rev 1 (3.1)](https://public.dhe.ibm.com/software/dw/webservices/ws-mqtt/mqtt-v3r1.html)

**Natiu**: Means *mosquito* in the [Guaraní language](https://en.wikipedia.org/wiki/Guarani_language), a language spoken primarily in Paraguay. Commonly written as ñati'û or ñati'ũ.

## Goals
This implementation will have a simple embedded-systems implementation in the package
top level. This implementation will be transport agnostic and non-concurrent. This will make it far easier to modify and reason about. The transport dependent implementations will have their own subpackage, so one package for TCP transport, another for UART, PPP etc.

* Minimal, if any, heap allocations (only using `unsafe` or `tinygo` build tags).
* Support for TCP transport.
* User owns payload bytes.

## Why "better-than-paho"?


Some issues with Eclipse's Paho implementation:
* [Inherent data races on API side](https://github.com/eclipse/paho.mqtt.golang/issues/550). The implementation is so notoriously hard to modify this issue has been in a frozen state.
* Calling Client.Disconnect when client is already disconnected blocks indefinetely and can cause deadlock or spin with Paho's implementation. 
* If there is an issue with the network and Reconnect is enabled then then Paho's Reconnect spins. There is no way to prevent this.
* Interfaces used for ALL data types. This is not necessary and makes it difficult to work with since there is no in-IDE documentation on interface methods.
* No lower level abstraction of MQTT for use in embedded systems with non-TCP transport.
* Uses `any` interface for the payload, which could simply be a byte slice...

I found these issues after a 2 hour dev session. There will undoubtedly be more if I were to try to actually get it working...