package kafka

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	"sync"
)

// Consumer
type Consumer struct {
	hosts        []string
	groupID      string
	topics       []string
	WorkNum      int
	handler      Handler
	Client       *cluster.Consumer
	saramaClient *cluster.Client
	wg           *sync.WaitGroup
}

// NewConsumer
func NewConsumer(hosts []string, groupID string, topics []string, workNum int) (*Consumer, error) {
	config := cluster.NewConfig()
	config.Group.Mode = cluster.ConsumerModePartitions
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true
	saramaClient, err := cluster.NewClient(hosts, config)
	if err != nil {
		return nil, err
	}
	client, err := cluster.NewConsumerFromClient(saramaClient, groupID, topics)
	if err != nil {
		return nil, err
	}

	return &Consumer{
		hosts:        hosts,
		groupID:      groupID,
		topics:       topics,
		Client:       client,
		WorkNum:      workNum,
		saramaClient: saramaClient,
		wg:           &sync.WaitGroup{},
	}, nil
}

//Close
func (consumer *Consumer) Close() {
	if consumer.Client == nil {
		return
	}
	err := consumer.Client.Close()
	if err != nil {
		return
	}
}

func (consumer *Consumer) Register(h Handler) {
	consumer.handler = h
}

//Run
func (consumer *Consumer) Run() {
	if consumer.Client == nil {
		return
	}
	defer consumer.wg.Wait()
	defer consumer.Close()
	for {
		select {
		case part, ok := <-consumer.Client.Partitions():
			if !ok {
				return
			}
			fmt.Println("Partition ", part.Partition())
			for workNum := 0; workNum < consumer.WorkNum; workNum++ {
				consumer.wg.Add(1)
				go consumer.readFromPart(part)
			}
		case err := <-consumer.Client.Errors():
			if err != nil {
				fmt.Println("consumer err", err)
			}
		case not := <-consumer.Client.Notifications():
			fmt.Println("consumer Notifications", not)
		}
	}
}

func (consumer *Consumer) readFromPart(pc cluster.PartitionConsumer) {
	defer consumer.wg.Done()
	for {
		select {
		case msg, ok := <-pc.Messages():
			if !ok {
				return
			}
			if consumer.handler.WorkHandler(msg.Value){
				consumer.Client.MarkOffset(msg, "")
			}
		}
	}
}
