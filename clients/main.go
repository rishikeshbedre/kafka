package main

import (
	"log"

	"kafka-client/producer"
	"kafka-client/consumer"
)

func main() {
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

	log.Println("Your data is stored with unique identifier important ", partition, offset)
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