package consumer

import (
	"context"
	"log"

	"github.com/Shopify/sarama"
)

type ConsumerGroup struct {
	ConsumerGroupClient sarama.ConsumerGroup
}

type KafkaConsumerGroup interface{
	Consume(topics []string) error
	WatchConsumerErrors()
	Close() error
}

func (cg *ConsumerGroup) Consume(topics []string) error {
	ctx := context.Background()
	for {
		handler := ConsumerGroupHandler{}

		err := cg.ConsumerGroupClient.Consume(ctx, topics, handler)
		if err != nil {
			return err
		}
	}
}

func (cg *ConsumerGroup) WatchConsumerErrors() {
	for err := range cg.ConsumerGroupClient.Errors() {
		log.Println(err)
	}
}

func (cg *ConsumerGroup) Close() error {
	err := cg.ConsumerGroupClient.Close()
	return err
}

func CreateNewConsumerGroup(brokerList []string, groupID string) (*ConsumerGroup, error) {
	config := sarama.NewConfig()
	config.Version = sarama.V2_1_0_0
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	consumergroup, err := sarama.NewConsumerGroup(brokerList, groupID, config)
	if err != nil {
		return nil, err
	}

	tempConsumerGroup := ConsumerGroup{
		ConsumerGroupClient: consumergroup,
	}
	return &tempConsumerGroup, nil
}