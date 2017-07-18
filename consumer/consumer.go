package main

import (
	"log"
	"os"
	"os/signal"

	"github.com/Shopify/sarama"
)

var autoIncrement int
var topic = "SalesProduct"
var brokers = []string{"localhost:9092"}

func main() {
	consumer := initConsumer()
	defer consumer.Close()

	partitionConsumer := createPartitionConsumer(consumer)
	defer partitionConsumer.Close()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	ch := make(chan struct{})

	go func() {
		for {
			select {
			case msg := <-partitionConsumer.Messages():
				log.Printf("[%v] msg received: (key, value) = (%v, %v)\n", autoIncrement, string(msg.Key), string(msg.Value))
				autoIncrement++
			case err := <-partitionConsumer.Errors():
				log.Println(err)
			case <-signals:
				ch <- struct{}{}
			}
		}
	}()

	<-ch
}

func initConsumer() sarama.Consumer {
	config := sarama.NewConfig()

	consumer, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		panic(err)
	}

	return consumer
}

func createPartitionConsumer(consumer sarama.Consumer) sarama.PartitionConsumer {
	partitionConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetNewest)
	if err != nil {
		panic(err)
	}

	return partitionConsumer
}
