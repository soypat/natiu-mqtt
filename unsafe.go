//go:build unsafe || tinygo

package mqtt

// Taken from
import "unsafe"

// unsafeSlice is the runtime representation of a unsafeSlice.
// It cannot be used safely or portably and its representation may
// change in a later release.
//
// Unlike reflect.SliceHeader, its Data field is sufficient to guarantee the
// data it references will not be garbage collected.
type unsafeSlice struct {
	Data unsafe.Pointer
	Len  int
	Cap  int
}

// unsafeString is the runtime representation of a string.
// It cannot be used safely or portably and its representation may
// change in a later release.
//
// Unlike reflect.StringHeader, its Data field is sufficient to guarantee the
// data it references will not be garbage collected.
type unsafeString struct {
	Data unsafe.Pointer
	Len  int
}

func bytesFromString(s string) []byte {
	var b []byte
	hdr := (*unsafeSlice)(unsafe.Pointer(&b))
	hdr.Data = (*unsafeString)(unsafe.Pointer(&s)).Data
	hdr.Cap = len(s)
	hdr.Len = len(s)
	return b
}
