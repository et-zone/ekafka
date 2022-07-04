package main

import (
	"fmt"
	"github.com/et-zone/ekafka/server"
)

var Address = []string{"118.195.250.202:9093"}
var topic = "gzy"


var count =1000
func main(){
	test_sync_Producer()
}
// 异步生产
func test_sync_Producer(){

	p:=server.NewDefaultProducer(Address)
	defer p.AsyncClose()

	for i:=1;i<=count;i++{
		msg:="sp:"+fmt.Sprintf("%v",i)
		p.Send(topic,[]byte(msg))
		//p.SendWithPartition(0,topic,[]byte(msg))
	}

	//time.Sleep(time.Second)
}

func test_Consumer(){

}
