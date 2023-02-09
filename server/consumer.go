package server

import (
	"context"
	"github.com/Shopify/sarama" // v1.29.0
	"log"
	"sync"
)

type Handler func(topic string, offset int64, msg []byte) error

//消费组本身就会平衡不同分区数据,故不能指定消费分区
//分组消费时,同一个消费group,可以同时消费,谁拿到谁消费
//消费组模式,每增加一个消费者,消费分区会平衡一次,此时会重新setup,
//消费者数量和分区数量有关,一个分区时2个消费组的情况,此时,一个消费者消费不到任何消息
// ==== 指定分区消费无意义,因为组模式的消费者数量决定了消费分区的情况 ====

func NewConsumerGroup(brokers, topics []string, group string, cfg *sarama.Config) *ConsumerGroup {
	c, err := sarama.NewConsumerGroup(brokers, group, cfg)
	ctx, cancel := context.WithCancel(context.Background())
	if err != nil {
		panic(err)
	}
	return &ConsumerGroup{
		WaitGroup: sync.WaitGroup{},
		Context:   ctx,
		client:    c,
		ready:     make(chan struct{}),
		close:     make(chan struct{}),
		Topics:    topics,
		cancel:    cancel,
	}
}

// Consumer represents a Sarama consumer group consumer
type ConsumerGroup struct {
	WaitGroup sync.WaitGroup
	Context   context.Context
	ready     chan struct{}
	close     chan struct{}
	Func      Handler
	client    sarama.ConsumerGroup
	Topics    []string
	cancel    context.CancelFunc
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (c *ConsumerGroup) Setup(s sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	log.Println("setup succ!")
	log.Println(s.Claims())
	c.ready <- struct{}{}
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (c *ConsumerGroup) Cleanup(s sarama.ConsumerGroupSession) error {
	s.Commit()
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (c *ConsumerGroup) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/Shopify/sarama/blob/main/consumer_group.go#L27-L29
	for {
		select {
		case message := <-claim.Messages():
			//log.Printf("Message claimed: value = %s, timestamp = %v, topic = %s", string(message.Value), message.Timestamp, message.Topic)
			if err := c.Func(message.Topic, message.Offset, message.Value); err == nil {
				session.MarkMessage(message, "")
			}
		// Should return when `session.Context()` is done.
		// If not, will raise `ErrRebalanceInProgress` or `read tcp <ip>:<port>: i/o timeout` when kafka rebalance. see:
		// https://github.com/Shopify/sarama/issues/1192
		case <-session.Context().Done():
			return nil
		}
	}
}

func (c *ConsumerGroup) syncWorker() {
	go func() {
		defer c.WaitGroup.Done()
		for {
			// `Consume` should be called inside an infinite loop, when a
			// server-side rebalance happens, the consumer session will need to be
			// recreated to get the new claims
			if err := c.client.Consume(c.Context, c.Topics, c); err != nil {
				log.Println("Error from consumer:", err)
			}
			// check if context was cancelled, signaling that the consumer should stop
			if c.Context.Err() != nil {
				c.close <- struct{}{}
				log.Println("Succ Done ")
				return
			}
		}
	}()

}
func (c *ConsumerGroup) Run() {
	c.WaitGroup.Add(1)
	c.syncWorker()
	select {
	case <-c.ready:
		break
	}
	log.Println("consumer group running...")
	c.WaitGroup.Wait()
}
func (c *ConsumerGroup) Register(f Handler) {
	if f == nil {
		log.Panic("register err f is nil")
	}
	c.Func = f
}

func (c *ConsumerGroup) Close() {
	c.cancel()
	select {
	case <-c.close:
		break
	}
	if err := c.client.Close(); err != nil {
		log.Println("Error closing client:", err)
	}
	log.Println("consumer group close succ! ")

}
