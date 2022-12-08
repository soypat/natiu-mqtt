//go:build !unsafe && !tinygo

package mqtt

func bytesFromString(s string) []byte {
	return []byte(s) // heap allocation ensured.
}
