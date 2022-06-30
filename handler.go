package kafka

type Handler interface {
	WorkHandler(msg []byte) bool
}
