package main

import (
	"fmt"
	"os"
	"os/signal"

	"github.com/Shopify/sarama"
)

func NewConsumer(hosts []string) (sarama.Consumer, error) {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	conn, err := sarama.NewConsumer(hosts, config)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func ConsumeTopic(topic string, consumer sarama.Consumer) {
	// Create partition consumer
	partitionConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetNewest)
	if err != nil {
		fmt.Printf("Failed to create partition consumer: %v", err)
	}
	defer partitionConsumer.Close()

	// Handle incoming messages
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	for {
		select {
		case msg := <-partitionConsumer.Messages():
			fmt.Printf("Received message: Topic=%s | Value=%s\n",
				msg.Topic, string(msg.Value))

		case <-signals:
			fmt.Println("Interrupt signal received, shutting down...")
			return
		}
	}
}

func main() {
	// Create new consumer
	hosts := []string{"localhost:9094"}
	consumer, err := NewConsumer(hosts)
	if err != nil {
		fmt.Println(err)
		return
	}
	if consumer != nil {
		fmt.Println("Consumer connected to Kafka successfully!")
	}
	defer consumer.Close()

	// Consume topic
	topic := "game-night"
	ConsumeTopic(topic, consumer)
}
