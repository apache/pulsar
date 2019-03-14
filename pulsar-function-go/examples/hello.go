package main

import (
	"context"
	"fmt"

	"github.com/apache/pulsar/pulsar-client-go/pulsar"
	"github.com/apache/pulsar/pulsar-function-go/instance"
)

func hello() {
	fmt.Println("hello pulsar function")
}

func main() {
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: "pulsar://localhost:6650",
	})
	if err != nil {
		fmt.Printf("error:%v\n",err)
	}
	defer client.Close()

	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: "topic1",
	})

	defer producer.Close()

	for i := 0; i < 10; i++ {
		if err := producer.Send(context.Background(), pulsar.ProducerMessage{
			Payload: []byte(fmt.Sprintf("hello-%d", i)),
		}); err != nil {
			fmt.Println("err")
		}
	}

	instance.Start(hello) // main.hello
}
