package main

import (
	"log"

	"kafka-client/producer"
)

func main() {
	producerClient, err := producer.CreateSyncProducer([]string{"localhost:9092"})
	if err != nil {
		log.Println(err)
	}
}