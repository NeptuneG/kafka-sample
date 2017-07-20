package producer

import (
	"encoding/json"
	"html/template"
	"log"
	"net/http"
	"strconv"
	"testing"
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

func (sp *SalesProductEncoder) Encode() ([]byte, error) {
	return json.Marshal(sp)
}

func (sp *SalesProductEncoder) Length() int {
	encoded, _ := json.Marshal(sp)
	return len(encoded)
}

var (
	autoIncrement int
	producer      Producer
	brokers     = []string{"localhost:9092"}
	topic       = "SalesProduct"
)

func TestProducer(t *testing.T) {

	// "async" - sarama.AsyncProducer
	// "sync"  - sarama.SyncProducer
	producer = GetProducer("async")
	defer producer.Close()

	producer.InitProducer(brokers)

	http.HandleFunc("/", dumbHandler)
	http.ListenAndServe(":9887", nil)

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

		err = producer.ProduceMessage(topic, &SalesProductEncoder{salesProduct})
		if err != nil {
			panic(err)
		}

		http.Redirect(w, r, "/", 302)
	}
}