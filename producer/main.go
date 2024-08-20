package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/gorilla/mux"
	"log"
	"net/http"
)

const kafkaBroker = "kafka:9093"

var kafkaTopic = "wallet-events"

type Event struct {
	App        string    `json:"app"`
	Type       string    `json:"type"`
	Time       string    `json:"time"`
	Meta       Meta      `json:"meta"`
	Wallet     string    `json:"wallet"`
	Attributes Attribute `json:"attributes"`
}

type Meta struct {
	User string `json:"user"`
}

type Attribute struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}

type EventPayload struct {
	Events []Event `json:"events"`
}

func dispatchEvents(w http.ResponseWriter, r *http.Request) {
	var eventPayload EventPayload
	if err := json.NewDecoder(r.Body).Decode(&eventPayload); err != nil {
		http.Error(w, "Invalid request payload", http.StatusBadRequest)
		return
	}

	producer, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": kafkaBroker})
	if err != nil {
		http.Error(w, "Failed to create producer", http.StatusInternalServerError)
		return
	}
	defer producer.Close()

	for _, event := range eventPayload.Events {
		value, err := json.Marshal(event)
		if err != nil {
			http.Error(w, "Failed to marshal event", http.StatusInternalServerError)
			return
		}

		producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &kafkaTopic, Partition: kafka.PartitionAny},
			Value:          value,
		}, nil)
	}

	producer.Flush(15 * 1000)

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Events dispatched"))
}

const (
	dsn = "user:password@tcp(localhost:3306)/teknasyon"
)

func displayState(w http.ResponseWriter, r *http.Request) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		http.Error(w, "Failed to connect to database", http.StatusInternalServerError)
		return
	}
	defer db.Close()
	rows, err := db.Query(`
		SELECT wallet, currency, SUM(amount) AS total_amount
		FROM transactions
		GROUP BY wallet, currency
	`)
	if err != nil {
		http.Error(w, "Failed to query database", http.StatusInternalServerError)
		return
	}
	defer rows.Close()
	type Balance struct {
		Currency string `json:"currency"`
		Amount   string `json:"amount"`
	}

	type Wallet struct {
		ID       string    `json:"id"`
		Balances []Balance `json:"balances"`
	}

	var walletsMap = make(map[string]*Wallet)

	for rows.Next() {
		var walletID, currency, amount string
		var totalAmount float64
		if err := rows.Scan(&walletID, &currency, &totalAmount); err != nil {
			http.Error(w, "Failed to read row", http.StatusInternalServerError)
			return
		}
		amount = fmt.Sprintf("%.2f", totalAmount)

		if wallet, ok := walletsMap[walletID]; ok {
			// Wallet mevcutsa, balance ekle
			wallet.Balances = append(wallet.Balances, Balance{Currency: currency, Amount: amount})
		} else {
			// Yeni wallet oluştur ve balance ekle
			walletsMap[walletID] = &Wallet{
				ID: walletID,
				Balances: []Balance{
					{Currency: currency, Amount: amount},
				},
			}
		}
	}

	// JSON formatında çıktı oluşturur
	wallets := []Wallet{}
	for _, wallet := range walletsMap {
		wallets = append(wallets, *wallet)
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(map[string][]Wallet{"wallets": wallets}); err != nil {
		http.Error(w, "Failed to encode JSON", http.StatusInternalServerError)
	}
}

func main() {
	router := mux.NewRouter()
	router.HandleFunc("/", dispatchEvents).Methods("POST") // isteği
	router.HandleFunc("/", displayState).Methods("GET")    // get isteği

	log.Fatal(http.ListenAndServe(":8080", router))
}
