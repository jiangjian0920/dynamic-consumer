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
	batchSize    = 100 // 调整批量插入的大小
)

func main() {
	// 设置Kafka消费者配置
	kafkaReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{kafkaBrokers},
		GroupID:  kafkaGroup,
		Topic:    kafkaTopic,
		MinBytes: 10e3, // 读取最小字节数
		MaxBytes: 10e6, // 读取最大字节数
	})

	// 连接到MySQL数据库
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatalf("无法连接到MySQL数据库: %v", err)
	}

	defer kafkaReader.Close()

	// 创建等待组，以等待所有 Goroutines 完成
	var wg sync.WaitGroup

	// 启动多个 Goroutines 并行处理 Kafka 消息
	for i := 0; i < 5; i++ { // 根据需求调整 Goroutine 数量
		wg.Add(1)
		go processMessages(&wg, kafkaReader, db)
	}

	// 等待所有 Goroutines 完成
	wg.Wait()
}

func processMessages(wg *sync.WaitGroup, r *kafka.Reader, db *gorm.DB) {
	defer wg.Done()

	// 准备一个切片来缓冲数据
	var records []map[string]interface{}

	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			log.Printf("读取Kafka消息时出错: %v", err)
			break
		}

		rawData := string(m.Value)

		// 解析JSON数据到 map
		var record map[string]interface{}
		if err := json.Unmarshal([]byte(rawData), &record); err != nil {
			log.Printf("解析JSON数据失败: %v", err)
			continue
		}

		records = append(records, record)

		// 当记录数达到批量插入大小时，执行批量插入操作
		if len(records) >= batchSize {
			if err := insertBatch(db, records); err != nil {
				log.Printf("批量插入到MySQL数据库时出错: %v", err)
			} else {
				log.Printf("成功批量插入数据到MySQL")
			}
			records = nil
		}
	}

	// 处理剩余的记录
	if len(records) > 0 {
		if err := insertBatch(db, records); err != nil {
			log.Printf("批量插入到MySQL数据库时出错: %v", err)
		} else {
			log.Printf("成功批量插入数据到MySQL")
		}
	}
}

// insertBatch 执行批量插入操作
func insertBatch(db *gorm.DB, records []map[string]interface{}) error {
	tx := db.Begin()
	for _, record := range records {
		// 将 map 数据序列化为 JSON 字符串
		jsonData, err := json.Marshal(record)
		if err != nil {
			tx.Rollback()
			return err
		}

		// 插入 JSON 字符串到数据库的 JSON 字段
		if err := tx.Exec(`INSERT INTO dynamic (data) VALUES (?)`, string(jsonData)).Error; err != nil {
			tx.Rollback()
			return err
		}
	}
	tx.Commit()
	return nil
}
