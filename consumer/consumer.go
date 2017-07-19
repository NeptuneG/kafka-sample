package main

import (
	"log"
	"os"
	"os/signal"

	"github.com/Shopify/sarama"
	"encoding/json"
)

// SalesProduct is the data model to be produced
type SalesProduct struct {
	Id          int
	ProductName string
	SalesDate   string
	SalesNumber int
}

var autoIncrement int
var topic = "SalesProduct"
var brokers = []string{"localhost:9092"}
var salesInfo map[string]int

func main() {
	salesInfo = make(map[string]int)

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
				consumeMessage(msg)
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

func consumeMessage(msg *sarama.ConsumerMessage) {
	log.Printf("[%v] msg received: (%v, %v)\n", autoIncrement, string(msg.Key), string(msg.Value))
	
	var salesProduct SalesProduct
	err := json.Unmarshal(msg.Value, &salesProduct)
	if err != nil {
		panic(err)
	}

	salesVolume := salesInfo[salesProduct.ProductName]
	salesInfo[salesProduct.ProductName] = salesVolume + salesProduct.SalesNumber

	log.Printf("Sales volume of %s: %v\n", salesProduct.ProductName, salesInfo[salesProduct.ProductName])
}