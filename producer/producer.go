package producer

import 	"github.com/Shopify/sarama"

type Producer interface {
	InitProducer(brokers []string)
	ProduceMessage(topic string, encoder sarama.Encoder) error
	Close() error
}

func GetProducer(name string) Producer {
	
	if name == "sync" {
		return &syncProducer{}
	}

	if name == "async" {
		return &asyncProducer{}
	}

	return nil
}