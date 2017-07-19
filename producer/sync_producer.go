package producer

import (
	"github.com/Shopify/sarama"
	"log"
)

type syncProducer struct {
	producer sarama.SyncProducer
}

func (sp *syncProducer) ProduceMessage(topic string, encoder sarama.Encoder) error {
	
	message := &sarama.ProducerMessage{
		Topic: topic,
		Value: encoder,
	}

	partition, offset, err := sp.producer.SendMessage(message)
	if err != nil {
		panic(err)
	}

	log.Printf("Partition: %v, Offset: %v\n", partition, offset)

	return nil
}

func (sp *syncProducer) InitProducer(brokers []string) {
	
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		panic(err)
	}
	
	sp.producer = producer
}

func (sp *syncProducer) Close() error {
	
	return sp.producer.Close()
}