package main

import (
	"encoding/json"
	"html/template"
	"log"
	"net/http"
	"strconv"

	"github.com/Shopify/sarama"
)

// SalesProduct is the data model to be produced
type SalesProduct struct {
	Id          int
	ProductName string
	SalesDate   string
	SalesNumber int
}

type SalesProductEncoder struct {
	SalesProduct
}

func (this *SalesProductEncoder) Encode() ([]byte, error) {
	return json.Marshal(this)
}

func (this *SalesProductEncoder) Length() int {
	encoded, _ := json.Marshal(this)
	return len(encoded)
}

var producer sarama.SyncProducer
var autoIncrement int
var topic = "SalesProduct"
var brokers = []string{"localhost:9092"}

func main() {

	producer = initSyncProducer()
	defer producer.Close()

	http.HandleFunc("/", dumbHandler)
	http.ListenAndServe(":9887", nil)

}

func initSyncProducer() sarama.SyncProducer {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		panic(err)
	}

	return producer
}

func dumbHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" {
		t, err := template.ParseFiles("index.html")
		if err != nil {
			panic(err)
		}
		err = t.Execute(w, nil)
		if err != nil {
			panic(err)
		}
	} else {
		productName := r.FormValue("ProductName")
		salesDate := r.FormValue("SalesDate")
		salesNumber, err := strconv.Atoi(r.FormValue("SalesNumber"))
		if err != nil {
			panic(err)
		}

		salesProduct := SalesProduct{
			Id:          autoIncrement,
			ProductName: productName,
			SalesDate:   salesDate,
			SalesNumber: salesNumber,
		}
		autoIncrement++

		log.Println(salesProduct)

		err = produceSyncMessage(producer, &SalesProductEncoder{salesProduct})
		if err != nil {
			panic(err)
		}

		http.Redirect(w, r, "/", 302)
	}
}

func produceSyncMessage(producer sarama.SyncProducer, encoder sarama.Encoder) error {
	message := &sarama.ProducerMessage{
		Topic: topic,
		Value: encoder,
	}

	partition, offset, err := producer.SendMessage(message)
	if err != nil {
		panic(err)
	}

	log.Printf("Partition: %v, Offset: %v\n", partition, offset)

	return nil
}
