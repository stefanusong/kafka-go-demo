package main

import (
	"fmt"

	"github.com/Shopify/sarama"
)

func NewProducer(hosts []string) (sarama.SyncProducer, error) {
	// Set producer configs
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5

	// Create new producer
	conn, err := sarama.NewSyncProducer(hosts, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka producer: %w", err)
	}
	return conn, nil
}

func PublishTopic(topic string, message string, producer sarama.SyncProducer) error {
	// Construct message
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}

	// Send message
	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		return fmt.Errorf("failed to publish topic: %w", err)
	}
	fmt.Printf("Message is stored in topic(%s)/partition(%d)/offset(%d)\n", topic, partition, offset)
	return nil
}

func main() {
	// Create new producer
	hosts := []string{"localhost:9094"}
	producer, err := NewProducer(hosts)
	if err != nil {
		fmt.Println(err)
		return
	}
	if producer != nil {
		fmt.Println("Producer connected to Kafka successfully!")
	}
	defer producer.Close()

	// Publish topic
	topic := "game-night"
	msg := "Let's Play"

	err = PublishTopic(topic, msg, producer)
	if err != nil {
		fmt.Println(err)
	}
}
