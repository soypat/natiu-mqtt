package mqtt

const (
	maxMessageHandlers      = 10
	defaultBufferLen        = 1500
	maxRemainingLengthSize  = 4
	maxRemainingLengthValue = 0xffff_ff7f
)

// Reserved flags for PUBREL, SUBSCRIBE and UNSUBSCRIBE packet types.
const flagsPubrelSubUnsub PacketFlags = 0b10
