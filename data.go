package mqtt

type Message struct {
	QoS        QoSLevel
	Retained   byte
	Duplicated byte
	ID         uint16
	Payload    []byte
	Topic      string
}
