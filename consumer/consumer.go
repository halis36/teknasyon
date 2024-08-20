package main

import (
	"database/sql"
	"encoding/json"
	"github.com/IBM/sarama" // Güncellenmiş kütüphane
	_ "github.com/go-sql-driver/mysql"
	"log"
	"strconv"
)

const (
	kafkaBroker = "172.20.0.3:9093"
	kafkaTopic  = "wallet-events"
	dbConnStr   = "user:password@tcp(localhost:3306)/teknasyon"
)

type Event struct {
	App  string `json:"app"`
	Type string `json:"type"`
	Time string `json:"time"`
	Meta struct {
		User string `json:"user"`
	} `json:"meta"`
	Wallet     string `json:"wallet"`
	Attributes struct {
		Amount   string `json:"amount"`
		Currency string `json:"currency"`
	} `json:"attributes"`
}

func main() {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	config.Version = sarama.V2_0_0_0

	consumer, err := sarama.NewConsumer([]string{kafkaBroker}, config)
	if err != nil {
		log.Fatalf("Consumer error: %v", err)
	}
	defer consumer.Close()

	partitionList, err := consumer.Partitions(kafkaTopic)
	if err != nil {
		log.Fatalf("Failed to get partition list: %v", err)
	}

	for _, partition := range partitionList {
		pc, err := consumer.ConsumePartition(kafkaTopic, partition, sarama.OffsetNewest)
		if err != nil {
			log.Fatalf("Failed to start consumer for partition %d: %v", partition, err)
		}
		defer pc.Close()

		go func(pc sarama.PartitionConsumer) {
			for msg := range pc.Messages() {
				var events struct {
					Events []Event `json:"events"`
				}
				if err := json.Unmarshal(msg.Value, &events); err != nil {
					log.Printf("Failed to unmarshal message: %v", err)
					continue
				}

				db, err := sql.Open("mysql", dbConnStr)
				if err != nil {
					log.Printf("Failed to connect to database: %s", err)
					continue
				}
				defer db.Close()

				for _, event := range events.Events {
					amount, err := parseFloat(event.Attributes.Amount)
					if err != nil {
						log.Printf("Failed to parse amount: %v", err)
						continue
					}

					var updateQuery string
					var updateArgs []interface{}

					if event.Type == "BALANCE_INCREASE" {
						updateQuery = "UPDATE transactions SET amount = amount + ? WHERE wallet = ? AND currency = ?"
						updateArgs = []interface{}{amount, event.Wallet, event.Attributes.Currency}
					} else if event.Type == "BALANCE_DECREASE" {
						updateQuery = "UPDATE transactions SET amount = amount - ? WHERE wallet = ? AND currency = ?"
						updateArgs = []interface{}{amount, event.Wallet, event.Attributes.Currency}
					} else {
						log.Printf("Unknown event type: %s", event.Type)
						continue
					}

					_, err = db.Exec(updateQuery, updateArgs...)
					if err != nil {
						log.Printf("Failed to update balance in database: %v", err)
					}
				}
			}
		}(pc)
	}

	select {}
}

func parseFloat(s string) (float64, error) {
	return strconv.ParseFloat(s, 64)
}
