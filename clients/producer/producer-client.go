package producer

import (
	"github.com/Shopify/sarama"
)

type Producer struct {
	SyncProducer sarama.SyncProducer
}

type KafkaProducer interface {
	ProduceMessage()
}

func (p *Producer) ProduceMessage() {

}

func CreateSyncProducer(brokerList []string) (*Producer, error) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 10
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer(brokerList, config)
	if err != nil {
		return nil, err
	}

	tempProducer := Producer{
		SyncProducer: producer,
	}
	return &tempProducer, nil
}