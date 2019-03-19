package main

import (
	"context"
	"fmt"
	"log"

	"github.com/apache/pulsar/pulsar-client-go/pulsar"
)

func main() {
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: "pulsar://localhost:6650",
	})

	if err != nil {
		log.Fatal(err)
		return
	}

	defer client.Close()

	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: "topic-01",
	})

	defer producer.Close()

	ctx := context.Background()

	for i := 0; i < 10; i++ {
		if err := producer.Send(ctx, pulsar.ProducerMessage{
			Payload: []byte(fmt.Sprintf("hello-%d", i)),
		}); err != nil {
			log.Fatal(err)
		}
	}
}
