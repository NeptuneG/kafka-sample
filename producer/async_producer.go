package producer

import (
	"github.com/Shopify/sarama"
	"log"
)

type asyncProducer struct {
	producer sarama.AsyncProducer
}

func (ap *asyncProducer) ProduceMessage(topic string, encoder sarama.Encoder) error {

	message := &sarama.ProducerMessage{
		Topic: topic,
		Value: encoder,
	}

	ap.producer.Input() <- message

	log.Println(message)
	
	return nil
}

func (ap *asyncProducer) InitProducer(brokers []string) {
	
	config := sarama.NewConfig()

	producer, err := sarama.NewAsyncProducer(brokers, config)
	if err != nil {
		panic(err)
	}
	
	ap.producer = producer
}

func (ap *asyncProducer) Close() error {
	
	return ap.producer.Close()
}