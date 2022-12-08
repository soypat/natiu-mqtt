package mqtt

import (
	"encoding/binary"
)

// Helper function for encoding a string for use in an MQTT control packet
// Courtesy of ChatGPT.
func newString(s string) []byte {
	// Encode the string length as a two-byte integer
	b := make([]byte, 2)
	binary.BigEndian.PutUint16(b, uint16(len(s)))

	// Append the string bytes to the encoded length
	return append(b, []byte(s)...)
}
