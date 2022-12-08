package mqtt

import (
	"io"
	"time"
)

type Client struct {
	nextPacketID    uint
	cmdTimeout      time.Duration
	buf             []byte
	readbuf         []byte
	keepalive       uint16
	pingOutstanding byte
	isConnected     int
	cleanSession    bool
	msgHandlers     [maxMessageHandlers]struct {
		topicFilter string
		handler     func(*Message)
	}
}

func NewClient(configuration ...ClientOption) (Client, error) {
	var cfg ClientConfig
	if len(configuration) == 0 {
		configuration = []ClientOption{DefaultClientConfig()}
	}
	for _, option := range configuration {
		option(&cfg)
		if cfg.err != nil {
			return Client{}, cfg.err
		}
	}
	return Client{
		nextPacketID: 1,
		readbuf:      cfg.ReadBuffer,
		buf:          cfg.WriteBuffer,
	}, nil
}

func (c *Client) connectFlags(username, password string, willRetain, willFlag bool, willQoS uint8) byte {
	hasUsername := username != ""
	return b2u8(hasUsername)<<7 | b2u8(hasUsername && password != "")<<6 | // See  [MQTT-3.1.2-22].
		b2u8(willRetain)<<5 | willQoS<<3 |
		b2u8(willFlag)<<2 | b2u8(c.cleanSession)<<1
}

func (c *Client) writeConnect(w io.Writer, payload []byte) error {
	var packet [11]byte
	packet[0] = ptConnect.marshal(0) // flags=0
	// remaining length
	rl := uint32(10 + len(payload))
	// n contains ptr to data.
	n := encodeRemainingLength(rl, packet[1:])
	n += 1
	// We encode the MQTT string (string of length 4)
	// Followed by Protocol level 4.
	n += copy(packet[n:], "\x00\x04MQTT\x04")
	// Followed by connect flags
	// packet[n] = c.connectFlags()
	packet[n+1] = byte(c.keepalive >> 8)
	packet[n+2] = byte(c.keepalive)
	n += 3
	_, err := w.Write(packet[:n])
	if err != nil {
		return err
	}
	return nil
}

// bool to uint8
func b2u8(b bool) uint8 {
	if b {
		return 1
	}
	return 0
}

type transport struct {
	readByte   func() (byte, error)
	sck        interface{}
	multiplier int
	remLen     int
	len        int
	state      uint8
}

func (trp *transport) readnb(b []byte) error {
	switch trp.state {
	default:
		trp.state = 0
		fallthrough
	case 0:
		frc, err := trp.readByte()
		if err != nil {
			trp.state = 0
			return err
		}
		if frc == 0 {
			return nil
		}
		trp.len = 0
		trp.state++
		fallthrough
	case 1:

	}
	return io.EOF
}

// decodes message length according to MQTT spec.
func (trp *transport) decodenb() (int, error) {
	var c byte

}
