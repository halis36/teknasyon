package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/IBM/sarama" // Güncellenmiş kütüphane
	_ "github.com/go-sql-driver/mysql"
	"log"
)

const (
	kafkaBroker = "172.20.0.3:9093"
	//kafkaBroker = "kafka:9092" // Kafka broker adresi
	//	kafkaBroker = "localhost:9092"                                  // Kafka broker adresi
	kafkaTopic = "wallet-events"                               // Kafka topic adı
	dbConnStr  = "user:password@tcp(localhost:3306)/teknasyon" // MySQL bağlantı dizesi
)

type Transaction struct {
	TransactionID string  `json:"transaction_id"`
	Amount        float64 `json:"amount"`
	Currency      string  `json:"currency"`
	UserID        string  `json:"user_id"`
	Timestamp     string  `json:"timestamp"`
	Wallet        string  `json:"wallet"`
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
				var transaction Transaction
				if err := json.Unmarshal(msg.Value, &transaction); err != nil {
					log.Printf("Failed to unmarshal message: %v", err)
					continue
				}

				fmt.Printf("Received transaction: %+v\n", transaction)

				// Veritabanına kaydetme işlemi
				db, err := sql.Open("mysql", dbConnStr)
				if err != nil {
					log.Printf("Failed to connect to database: %s", err)
					continue
				}
				defer db.Close()

				_, err = db.Exec("INSERT INTO transactions (transaction_id, amount, currency, user_id, wallet, timestamp) VALUES (?, ?, ?, ?, ?, ?)",
					transaction.TransactionID, transaction.Amount, transaction.Currency, transaction.UserID, transaction.Wallet, transaction.Timestamp)
				if err != nil {
					log.Printf("Failed to insert transaction into database: %v", err)
				}
			}
		}(pc)
	}

	// Uygulamanın sonlandırılmasını bekle
	select {}
}
