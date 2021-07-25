package main

import (
	"log"
	"os"
	"time"

	"kafka-client/consumer"
	"kafka-client/producer"
	"github.com/Shopify/sarama"
)

func main() {
	sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)

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

	//-----------------------------------Consumer Group Example------------------------------------------

	createTopic("food", 2)

	var asyncProducerClient2 producer.KafkaProducerAsync
	asyncProducerClient2, asyncClientErr2 := producer.CreateAsyncProducer([]string{"marvelvm:9092"})
	if asyncClientErr2 != nil {
		log.Println(asyncClientErr2)
		return
	}

	go asyncProducerClient2.WatchProducerSuccesses()
	go asyncProducerClient2.WatchProducerErrors()

	go func() {
		for i := 0; i < 5; i++ {
			// asyncProducerClient2.ProduceMessageWithPartition("food", "starter", "chicken wings", 0)
			// asyncProducerClient2.ProduceMessageWithPartition("food", "salad", "caesar salad", 1)
			asyncProducerClient2.ProduceMessage("food", "0", "chicken wings")
			asyncProducerClient2.ProduceMessage("food", "1", "caesar salad")
		}

		time.Sleep(50 * time.Second)
		pAyncCloseErr2 := asyncProducerClient2.Close()
		if pAyncCloseErr2 != nil {
			log.Println(pAyncCloseErr2)
			return
		}
	}()

	var consumergroupclient consumer.KafkaConsumerGroup
	consumergroupclient, cgClientErr := consumer.CreateNewConsumerGroup([]string{"marvelvm:9092"}, "foodconsumer", "food-starter")
	if cgClientErr != nil {
		log.Println(cgClientErr)
		return
	}

	go consumergroupclient.WatchConsumerErrors()

	go func() {
		cgConsumeErr := consumergroupclient.Consume([]string{"food"})
		if cgConsumeErr != nil {
			log.Println(cgConsumeErr)
			return
		}
	}()


	var consumergroupclient2 consumer.KafkaConsumerGroup
	consumergroupclient2, cgClientErr2 := consumer.CreateNewConsumerGroup([]string{"marvelvm:9092"}, "foodconsumer", "food-salad")
	if cgClientErr2 != nil {
		log.Println(cgClientErr2)
		return
	}

	go consumergroupclient2.WatchConsumerErrors()

	go func() {
		cgConsumeErr2 := consumergroupclient2.Consume([]string{"food"})
		if cgConsumeErr2 != nil {
			log.Println(cgConsumeErr2)
			return
		}
	}()


	time.Sleep(60 * time.Second)
	cgCloseErr := consumergroupclient.Close()
	if cgCloseErr != nil {
		log.Println(cgCloseErr)
		return
	}

	cgCloseErr2 := consumergroupclient2.Close()
	if cgCloseErr2 != nil {
		log.Println(cgCloseErr2)
		return
	}

}

func createTopic(topic string, partition int32) {
	brokerAddrs := []string{"marvelvm:9092"}
    config := sarama.NewConfig()
    config.Version = sarama.V2_1_0_0
    admin, err := sarama.NewClusterAdmin(brokerAddrs, config)
    if err != nil {
        log.Println("Error while creating cluster admin: ", err.Error())
    }
    defer func() { _ = admin.Close() }()
    err = admin.CreateTopic(topic, &sarama.TopicDetail{
        NumPartitions:     partition,
        ReplicationFactor: 1,
    }, false)
    if err != nil {
        log.Println("Error while creating topic: ", err.Error())
    }
}
