package main

import (
	"StockSymbolAnalyzer/configuration"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
)

const TopicConfiguration = "kafka.topic"

func main() {

	if len(os.Args) != 3 {
		fmt.Fprintf(os.Stderr, "Usage: %s <kafka-config-file-path> <app-config-file-path\n", os.Args[0])
		os.Exit(1)
	}

	kafkaConfigFile := os.Args[1]
	configFile := os.Args[2]

	kafkaConf := configuration.ReadKafkaConfig(kafkaConfigFile)
	appConfig := configuration.ReadConfiguration(configFile)
	c, err := kafka.NewConsumer(&kafkaConf)

	if err != nil {
		fmt.Printf("Failed to create consumer: %s", err)
		os.Exit(1)
	}

	p, err := kafka.NewProducer(&kafkaConf)
	if err != nil {
		fmt.Printf("Failed to create producer: %s", err)
		os.Exit(1)
	}

	// Go-routine to handle message delivery reports and
	// possibly other event types (errors, stats, etc)
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Failed to deliver message: %v\n", ev.TopicPartition)
				} else {
					fmt.Printf("Produced event to topic %s: key = %-10s value = %s\n",
						*ev.TopicPartition.Topic, string(ev.Key), string(ev.Value))
				}
			}
		}
	}()

	// can only be added afterwards as the NewConsumer cannot handle the values
	// var topic = conf[TopicConfiguration]
	err = c.SubscribeTopics(
		[]string{appConfig.Kafka.ReadTopic},
		nil,
	)

	// Set up a channel for handling Ctrl-C, etc
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	// Process messages
	run := true
	for run {
		select {
		case sig := <-sigchan:
			fmt.Printf("Caught signal %v: terminating\n", sig)
			run = false
		default:
			ev, err := c.ReadMessage(100 * time.Millisecond)
			if err != nil {
				// Errors are informational and automatically handled by the consumer
				continue
			}
			fmt.Printf("Consumed event from topic %s: key = %-10s value = %s paritition = %s position = %s timestampt = %s \n",
				*ev.TopicPartition.Topic, string(ev.Key), string(ev.Value), string(ev.TopicPartition.Partition), ev.TopicPartition.Offset.String(), ev.Timestamp.String())

			currValue, err := strconv.ParseFloat(string(ev.Value[:]), 32)
			newValue := currValue + 100
			fmt.Printf("Writing new value %f instead of current value %f to topic %s\n", newValue, currValue, appConfig.Kafka.WriteTopic)

			p.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &appConfig.Kafka.WriteTopic, Partition: kafka.PartitionAny},
				Key:            []byte(ev.Key),
				Value:          []byte(fmt.Sprintf("%f", newValue)),
			}, nil)
		}
	}

	c.Close()

}
