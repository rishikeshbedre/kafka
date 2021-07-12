package consumer

import (
	"log"
	"sync"

	"github.com/Shopify/sarama"
)

type Consumer struct {
	ConsumerClient sarama.Consumer
	Wg sync.WaitGroup
}

type KafkaConsumer interface {
	ConsumeMessage(topic string) error
	Close()
}

func (c *Consumer) ConsumeMessage(topic string) error {
	partitionList, getPartitionErr := c.ConsumerClient.Partitions(topic)
	if getPartitionErr != nil {
		return getPartitionErr
	}
	log.Println("Partition List for the Topic:", topic, " is ", partitionList)
	
	for partition := range partitionList {
		partitionConsumer, consumePartitionErr := c.ConsumerClient.ConsumePartition(topic, int32(partition), sarama.OffsetOldest)
		if consumePartitionErr != nil {
			return consumePartitionErr
		}
		
		c.Wg.Add(1)
		go c.ConsumePartitionMessages(partitionConsumer)
	}

	c.Wg.Wait()
	return nil
}

func (c *Consumer) ConsumePartitionMessages(partitionConsumer sarama.PartitionConsumer) {
	for msg := range partitionConsumer.Messages() {
		log.Printf("Partition:%d, Offset:%d, Key:%s, Value:%s", msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))
	}

	err := partitionConsumer.Close()
	if err != nil {
		log.Println(err)
	}

	c.Wg.Done()
}

func (c *Consumer) Close() error {
	err := c.ConsumerClient.Close()
	return err
}

func CreateNewConsumer(brokerList []string) (*Consumer, error) {
	config := sarama.NewConfig()

	consumer, err := sarama.NewConsumer(brokerList, config)
	if err != nil {
		return nil, err
	}

	tempConsumer := Consumer{
		ConsumerClient: consumer,
		Wg: sync.WaitGroup{},
	}
	return &tempConsumer, nil
}