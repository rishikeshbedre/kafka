package main

import (
	"log"
	//"os"

	"kafka-client/producer"
	"kafka-client/consumer"

	//"github.com/Shopify/sarama"
)

func main() {
	//sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)

	var syncproducerClient producer.KafkaProducerSync
	syncproducerClient, clientErr := producer.CreateSyncProducer([]string{"marvelvm:9092"})
	if clientErr != nil {
		log.Println(clientErr)
		return
	}

	partition, offset, produceErr := syncproducerClient.ProduceMessage("desert", "jamun")
	if produceErr != nil {
		log.Println(produceErr)
		return
	}

	log.Println("Your data is stored with unique identifier important ", partition, offset)
	pCloseErr := syncproducerClient.Close()
	if pCloseErr != nil {
		log.Println(pCloseErr)
		return
	}


	var asyncProducerClient producer.KafkaProducerAsync
	asyncProducerClient, asyncClientErr := producer.CreateAsyncProducer([]string{"marvelvm:9092"})
	if asyncClientErr != nil {
		log.Println(asyncClientErr)
		return
	}

	go asyncProducerClient.WatchProducerSuccesses()
	go asyncProducerClient.WatchProducerErrors()

	asyncProducerClient.ProduceMessage("desert", "favourite", "rasmalai")

	pAyncCloseErr := asyncProducerClient.Close()
	if pAyncCloseErr != nil {
		log.Println(pAyncCloseErr)
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