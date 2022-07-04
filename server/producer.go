package server

import (
	"fmt"

	"log"
	"time"

	"github.com/Shopify/sarama"
)

//同步生产模式
// 同步生产底层调用还是异步的生产者,,故不封装同步的API
//-------------------------------------------------------------------

//异步生产模式

type Producer struct {
	AsPro sarama.AsyncProducer
	close chan struct{}
	count int64
}

func NewDefaultProducer(address []string) *Producer {
	producer := &Producer{
		close: make(chan struct{}, 0),
	}
	config := sarama.NewConfig()
	config.Producer.Timeout = 5 * time.Second //超时事件５秒
	// config.Producer.Idempotent = true         //设置幂等性
	//config.Version = kafka_version
	//幂等性
	config.Producer.Idempotent = true
	config.Net.MaxOpenRequests = 1 //允许阻塞的请求数必须是1
	// config.Producer.Compression = sarama.CompressionSnappy //是否压缩数据－指定格式snappy　或ｚｉｐ等
	// config.Producer.Flush.Frequency  //刷新频率
	// 等待服务器所有副本都保存成功后的响应=====**××*×*××××*××*×××××
	config.Producer.RequiredAcks = sarama.WaitForAll
	// 随机的分区类型：返回一个分区器，该分区器每次选择一个随机分区
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	//config.Producer.Partitioner = sarama.NewManualPartitioner //指定分区生产需要选择这个
	// 异步判断是否等待成功和失败后的响应 这里没有使用异步  //生产者发送消息成功会返回ｔｒｕｅ
	config.Producer.Return.Successes = true

	//使用配置,新建一个异步生产者
	p, err := sarama.NewAsyncProducer(address, config)
	if err != nil {
		fmt.Println(err.Error())
		return nil
	}
	producer.AsPro = p
	go producer.confirm()
	return producer
}

func NewProducer(address []string, config *sarama.Config) *Producer {
	producer := &Producer{
		close: make(chan struct{}),
	}
	p, err := sarama.NewAsyncProducer(address, config)
	if err != nil {
		log.Fatalln("NewsyncProducer err: ", err.Error())
		return nil
	}
	producer.AsPro = p
	go producer.confirm()
	return producer
}

func (this *Producer) AsyncClose() {
	time.Sleep(time.Millisecond * 1000)
	this.close <- struct{}{}
	this.AsPro.AsyncClose()
	time.Sleep(time.Millisecond * 10)
}

//func (this *KafASyncProducer) Close() {
//	this.Producer.Close()
//}

//异步确认消息,使用go xxx执行
func (this *Producer) confirm() {
	for {
		select {
		case suc := <-this.AsPro.Successes(): //异步确认消息发送成功
			if suc == nil {
				fmt.Println("suc==nil")
			} else {
				fmt.Println(fmt.Sprintf("异步生产: offset:%d 分区: %d ", suc.Offset, suc.Partition))
			}
		case fail := <-this.AsPro.Errors():
			fmt.Println("err: ", fail.Err)
		case <-this.close:
			fmt.Println("confirm worker close succ!")
			return
		}
	}
}

//send message
func (this *Producer) Send(topic string, msg []byte) error {
	this.AsPro.Input() <- &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(msg),
	}
	return nil
}

//send message by partition
func (this *Producer) SendWithPartition(partition int32, topic string, msg []byte) error {
	this.AsPro.Input() <- &sarama.ProducerMessage{
		Topic:     topic,
		Value:     sarama.ByteEncoder(msg),
		Partition: partition,
	}
	return nil
}
