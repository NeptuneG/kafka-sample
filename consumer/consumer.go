package main

import (
	"log"
	"os"
	"os/signal"
	"encoding/json"

	"github.com/Shopify/sarama"
	"github.com/garyburd/redigo/redis"
	"time"
)

// SalesProduct is the data model to be produced
type SalesProduct struct {
	Id          int
	ProductName string
	SalesDate   string
	SalesNumber int
}

var (
	autoIncrement int
	consumer      sarama.Consumer
	pool          *redis.Pool
	brokers    = []string{"localhost:9092"}
	topic      = "SalesProduct"
	setName    = "sales"
	redisHost  = "192.168.77.137:6379"
)

func initRedisPool() *redis.Pool {
	return &redis.Pool{
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", redisHost)
			if err != nil {
				return nil, err
			}
			return c, err
		},
		IdleTimeout: 240 * time.Second,
		MaxIdle:     3,
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
}

func main() {
	
	pool = initRedisPool()
	defer pool.Close()

	consumer = initConsumer()
	defer consumer.Close()

	partitionConsumer := createPartitionConsumer()
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

func createPartitionConsumer() sarama.PartitionConsumer {
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
	log.Println(salesProduct)

	key := salesProduct.ProductName
	value := salesProduct.SalesNumber

	updateRedis(key, value)
}

func updateRedis(key string, value int) {
	conn := pool.Get()
	defer conn.Close()
	
	salesVolume, err := redis.Int(conn.Do("ZSCORE", setName, key))
	if err != nil && err.Error() != "redigo: nil returned" {
		panic(err)	
	}

	value += salesVolume

	log.Printf("merge (%s, %v) to set: %s\n", key, value, setName)
	
	// ZADD key [NX|XX] [CH] [INCR] score member [score member ...]
	_, err = conn.Do("ZADD", setName, value, key)
	if err != nil {
		panic(err)
	}
}