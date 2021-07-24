package producer

import (
	"github.com/Shopify/sarama"
)

type ProducerSync struct {
	SyncProducer sarama.SyncProducer
}

type KafkaProducerSync interface {
	ProduceMessage(topic string, message string) (int32, int64, error)
	Close() error
}

func (p *ProducerSync) ProduceMessage(topic string, message string) (int32, int64, error) {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}

	partition, offset, err := p.SyncProducer.SendMessage(msg)
	return partition, offset, err
}

func (p *ProducerSync) Close() error {
	err := p.SyncProducer.Close()
	return err
}

func CreateSyncProducer(brokerList []string) (*ProducerSync, error) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 10
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer(brokerList, config)
	if err != nil {
		return nil, err
	}

	tempProducer := ProducerSync{
		SyncProducer: producer,
	}
	return &tempProducer, nil
}
