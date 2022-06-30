package kafka

import (
	"github.com/Shopify/sarama"
	"log"
)

var (
	producer sarama.AsyncProducer
	Client   sarama.Client
	quit     = make(chan struct{})
)

func InitProducer(hosts []string) {
	var err error
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	Client, err = sarama.NewClient(hosts, config)
	if err != nil {
		panic(err.Error())
	}

	producer, err = sarama.NewAsyncProducerFromClient(Client)
	if err != nil {
		panic(err.Error())
	}

	go func(p sarama.AsyncProducer, quit chan struct{}) {
		for {
			select {
			case _ = <-p.Successes():
			//case  suc := <-p.Successes():
			//	log.Println("offset: ", suc.Offset, "timestamp: ", suc.Timestamp.String(), "partitions: ", suc.Partition)
			case fail := <-p.Errors():
				log.Println("AsyncProducer receive err: ", fail.Err)
			case <-quit:
				log.Println("Producer will close...")
				return
			}
		}
	}(producer, quit)
}
func CloseProducer() {
	quit <- struct{}{}
	if producer != nil {
		err := producer.Close()
		if err != nil {
			log.Println("close producer error:", err.Error())
		}
	}
	if Client != nil {
		err := Client.Close()
		if err != nil {
			log.Println("close producer error:", err.Error())
		}
	}
	log.Println("Producer close  success")
}

func Send(val []byte, topic string) (err error) {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(val),
	}
	producer.Input() <- msg
	return nil
}
