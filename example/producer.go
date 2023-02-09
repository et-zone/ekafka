package main

import (
	"fmt"
	"github.com/et-zone/ekafka/server"
	"time"
)

var brokers = []string{"43.137.40.241:9093"}
var topic = "test"

var count = 1000

func main() {
	test_sync_Producer()
}

// 异步生产
func test_sync_Producer() {

	p := server.NewDefaultProducer(brokers)
	defer p.Close()

	for i := 900; i <= count; i++ {
		msg := "sp:" + fmt.Sprintf("%v", i)
		p.Send(topic, []byte(msg))
		fmt.Println(msg)
		time.Sleep(time.Second)
		//p.SendWithPartition(0,topic,[]byte(msg))
	}

	//time.Sleep(time.Second)
}

func test_Consumer() {

}
