package main

import (
	"context"
	"encoding/json"
	"github.com/segmentio/kafka-go"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"log"
	"sync"
)

const (
	kafkaBrokers = "192.168.59.130:9092"
	kafkaTopic   = "RpaTaskAnalysis-dynamic"
	kafkaGroup   = "RpaTaskAnalysis-dynamic"
	dsn          = "root:123456@tcp(192.168.59.130:3306)/dynamic"
)

func main() {
	// Set up Kafka consumer configuration.
	kafkaReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{kafkaBrokers},
		GroupID:  kafkaGroup,
		Topic:    kafkaTopic,
		MinBytes: 10e3,
		MaxBytes: 10e6,
	})

	// Connect to the MySQL database.
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatalf("Failed to connect to MySQL database: %v", err)
	}

	defer kafkaReader.Close()

	// Create a wait group to wait for all goroutines to finish.
	var wg sync.WaitGroup

	// Start multiple goroutines to process Kafka messages in parallel.
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go processMessages(&wg, kafkaReader, db)
	}

	// Wait for all goroutines to finish.
	wg.Wait()
}

func processMessages(wg *sync.WaitGroup, r *kafka.Reader, db *gorm.DB) {
	defer wg.Done()

	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			log.Printf("Error reading Kafka message: %v", err)
			break
		}

		rawData := string(m.Value)

		var record map[string]interface{}
		if err := json.Unmarshal([]byte(rawData), &record); err != nil {
			log.Printf("Error parsing JSON data: %v", err)
			continue
		}

		// Process the message and insert it into the database.
		if err := processAndInsert(db, record); err != nil {
			log.Printf("Error inserting data into MySQL: %v", err)
		} else {
			log.Printf("Successfully inserted data into MySQL")
			// Commit the Kafka offset only after successfully processing and inserting the message.
			if err := r.CommitMessages(context.Background(), m); err != nil {
				log.Printf("Error committing Kafka offset: %v", err)
			}
		}
	}
}

func processAndInsert(db *gorm.DB, record map[string]interface{}) error {
	tx := db.Begin()

	jsonData, err := json.Marshal(record)
	if err != nil {
		tx.Rollback()
		return err
	}

	if err := tx.Exec("INSERT INTO dynamic (data) VALUES (?)", string(jsonData)).Error; err != nil {
		tx.Rollback()
		return err
	}

	tx.Commit()
	return nil
}
