package mqtt

import (
	"encoding/binary"
)

func NewConnectPacket(clientID string, cleanSession bool, keepAlive uint16) *controlPacket {
	// Create the CONNECT packet payload
	payload := []byte{0x00, 0x06}                                  // Protocol name and level
	payload = append(payload, 0x00)                                // Connect flags
	payload = append(payload, byte(keepAlive>>8), byte(keepAlive)) // Keep alive
	payload = append(payload, newString(clientID)...)              // Client ID
	// Return a new Packet with the CONNECT packet type and payload
	return &controlPacket{PacketType: 1, Payload: payload}
}

// Helper function for encoding a string for use in an MQTT control packet
// Courtesy of ChatGPT.
func newString(s string) []byte {
	// Encode the string length as a two-byte integer
	b := make([]byte, 2)
	binary.BigEndian.PutUint16(b, uint16(len(s)))

	// Append the string bytes to the encoded length
	return append(b, []byte(s)...)
}
