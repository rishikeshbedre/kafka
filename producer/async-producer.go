package producer

import (
	"log"

	"github.com/Shopify/sarama"
)

type ProducerAsync struct {
	AsyncProducer sarama.AsyncProducer
}

type KafkaProducerAsync interface {
	ProduceMessage(topic string, key string, message string)
	WatchProducerSuccesses()
	WatchProducerErrors()
	Close() error
}

func (p *ProducerAsync) ProduceMessage(topic string, key string, message string) {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key: sarama.StringEncoder(key),
		Value: sarama.StringEncoder(message),
	}

	p.AsyncProducer.Input() <- msg
}

func (p *ProducerAsync) WatchProducerSuccesses() {
	for msg := range p.AsyncProducer.Successes() {
		log.Println("Success to produce:", msg)
	}
}

func (p *ProducerAsync) WatchProducerErrors() {
	for err := range p.AsyncProducer.Errors() {
		log.Println("Failed to produce:", err)
	}
}

func (p *ProducerAsync) Close() error {
	err := p.AsyncProducer.Close()
	return err
}

func CreateAsyncProducer(brokerList []string) (*ProducerAsync, error) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForLocal
	config.Producer.Retry.Max = 10
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true

	producer, err := sarama.NewAsyncProducer(brokerList, config)
	if err != nil {
		return nil, err
	}

	tempProducer := ProducerAsync {
		AsyncProducer: producer,
	}
	return &tempProducer, nil
}