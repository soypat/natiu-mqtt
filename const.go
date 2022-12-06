package mqtt

const (
	maxMessageHandlers      = 10
	defaultBufferLen        = 1500
	maxRemainingLengthSize  = 4
	maxRemainingLengthValue = 0xffff_ff7e
)

const (
	QoS0 uint8 = iota
	QoS1
	QoS2
	QoSSubfail uint8 = 0x80
)
