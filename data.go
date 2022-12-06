package mqtt

type Message struct {
	QoS        uint8
	Retained   byte
	Duplicated byte
	ID         uint16
	Payload    []byte
	Topic      string
}

type connackData struct {
	rc             byte
	sessionPresent byte
}

type subackData struct {
	grantedQoS uint8
}
