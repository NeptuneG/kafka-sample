package main

import (
	"fmt"
	"html/template"
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

func main() {

	producer, err := initProducer()
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	http.HandleFunc("/", handlerMaker(producer, dumbHandler))
	http.ListenAndServe(":9887", nil)

}

func initProducer() (sarama.AsyncProducer, error) {
	return nil, nil
}

func handlerMaker(producer sarama.AsyncProducer, handler http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

	}
}

func persistSalesProduct(record SalesProduct) {
	fmt.Println(record)
}

func dumbHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" {
		t, err := template.ParseFiles("index.html")
		if err != nil {
			fmt.Println(err.Error())
			return
		}
		err = t.Execute(w, nil)
		if err != nil {
			fmt.Println(err.Error())
			return
		}
	} else {
		productName := r.FormValue("ProductName")
		salesDate := r.FormValue("SalesDate")
		salesNumber, err := strconv.Atoi(r.FormValue("SalesNumber"))
		if err != nil {
			fmt.Println(err.Error())
			return
		}

		salesProduct := &SalesProduct{
			ProductName: productName,
			SalesDate:   salesDate,
			SalesNumber: salesNumber,
		}

		persistSalesProduct(*salesProduct)

		http.Redirect(w, r, "/", 302)
	}
}
