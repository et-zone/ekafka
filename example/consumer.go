package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/et-zone/ekafka/server"
	"time"
)

var brokers = []string{"192.168.0.112:9092"}
var topics = []string{"test"}

func main() {
	cfg := sarama.NewConfig()
	cfg.Consumer.Offsets.Initial = sarama.OffsetNewest
	cfg.Consumer.Offsets.AutoCommit.Enable = false
	//分区平衡消费模式
	//cfg.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategySticky
	//cfg.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	//cfg.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRange

	c := server.NewConsumerGroup(brokers, topics, "test", cfg)
	c.Register(Do)
	go c.Run()
	time.Sleep(time.Second * 10)
	c.Close()

}

func Do(topic string, offset int64, msg []byte) error {
	fmt.Println(fmt.Sprintf("topic=%v, offset=%v, msg=%v ", topic, offset, string(msg)))
	//return errors.New("fsdaf")
	return nil
}

//指定分区消费
//func main() {
//	// init (custom) config, set mode to ConsumerModePartitions
//	config := sarama.NewConfig()
//	config.Version, _ = sarama.ParseKafkaVersion("2.3.0")
//	// config.Consumer.Offsets.AutoCommit = false 自动提交可以不用设置，在kafka内直接配置
//	config.Consumer.Offsets.Initial = sarama.OffsetOldest //默认OffsetNewest(跳过老数据)
//	// init consumer
//	brokers := []string{"49.234.123.136:9095", "49.235.235.2:9095", "212.64.35.115:9095"}
//
//	client, err := sarama.NewConsumer(brokers, config)
//	if err != nil {
//		log.Panicf("Error creating consumer group client: %v", err)
//	}
//	defer client.Close()
//	partitions, _ := client.Partitions("abc")
//	fmt.Println(partitions)
//	//相当于new一个客户端连接来使用
//	//只能指定一个topic
//
//	for partition := range partitions {
//		//OffsetNewest==只接受最新的，错过了就不管，old==全都要读出来,写多少就从哪里开始，
//		pc, _ := client.ConsumePartition("abc", int32(partition), sarama.OffsetOldest)
//		//方案:操作完之后，将获取的offset存储到redis，或者本地，每次操作，先读取该offset，没有就重头开始
//		defer pc.AsyncClose()
//		pc.HighWaterMarkOffset()
//		// 异步从每个分区消费信息
//		go func(sarama.PartitionConsumer) {
//			for msg := range pc.Messages() {
//				log.Printf("Partition:%d Offset:%d Key:%v Value:%s", msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))
//			}
//		}(pc)
//	}
//
//	sigterm := make(chan os.Signal, 1)
//	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
//	select {
//	case <-sigterm:
//		log.Println("terminating: via signal")
//	}
//
//}
