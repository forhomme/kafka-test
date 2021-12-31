package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost",
		"group.id":          "myGroup",
		"auto.offset.reset": "earliest",
	})

	if err != nil {
		panic(err)
	}
	fmt.Printf("Start Consumer\n")

	c.SubscribeTopics([]string{"myTopic", "myTopic2"}, nil)

	for {
		msg, err := c.ReadMessage(-1)
		if err != nil {
			// The client will automatically try to recover from all errors.
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
		} else {
			switch *msg.TopicPartition.Topic {
			case "myTopic2":
				fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))
			}
		}
	}

	c.Close()
}
