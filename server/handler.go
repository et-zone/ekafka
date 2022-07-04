package server

type Handler func(topic string,offset int64,msg []byte)error
