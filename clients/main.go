package main

import (
	"log"
	"os"

	"kafka-client/producer"
	"kafka-client/consumer"

	"github.com/Shopify/sarama"
)

func main() {
	sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)

	producerClient, clientErr := producer.CreateSyncProducer([]string{"marvelvm:9092"})
	if clientErr != nil {
		log.Println(clientErr)
		return
	}

	partition, offset, produceErr := producerClient.ProduceMessage("desert", "jamun")
	if produceErr != nil {
		log.Println(produceErr)
		return
	}

	sarama.Logger.Println("Your data is stored with unique identifier important ", partition, offset)
	pCloseErr := producerClient.Close()
	if pCloseErr != nil {
		log.Println(pCloseErr)
		return
	}


	consumerClient, clientErr := consumer.CreateNewConsumer([]string{"marvelvm:9092"})
	if clientErr != nil {
		log.Println(clientErr)
		return
	}

	consumeErr := consumerClient.ConsumeMessage("desert")
	if consumeErr != nil {
		log.Println(consumeErr)
		return
	}

	cCloseErr := consumerClient.Close()
	if cCloseErr != nil {
		log.Println(cCloseErr)
		return
	}
}