package producer

import (
	"github.com/Shopify/sarama"
)

type Producer struct {
	SyncProducer sarama.SyncProducer
}

type KafkaProducer interface {
	ProduceMessage(topic string, message string) (int32, int64, error)
	Close()
}

func (p *Producer) ProduceMessage(topic string, message string) (int32, int64, error) {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}

	partition, offset, err := p.SyncProducer.SendMessage(msg)
	return partition, offset, err
}

func (p *Producer) Close() error {
	err := p.SyncProducer.Close()
	return err
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
