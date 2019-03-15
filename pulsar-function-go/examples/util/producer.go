package util

import (
	"fmt"

	"github.com/apache/pulsar/pulsar-client-go/pulsar"
)

func SetProducer() {
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: "pulsar://localhost:6650",
	})
	if err != nil {
		fmt.Printf("error:%v\n", err)
	}
	defer client.Close()

	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		//ensure topic name equal inputSpecs of conf.yaml
		Topic: "topic-3",
	})
	defer producer.Close()

	//ctx := context.Background()
	//for i := 0; i < 10; i++ {
	//	// Create a different message to send asynchronously
	//	asyncMsg := pulsar.ProducerMessage{
	//		Payload: []byte(fmt.Sprintf("async-message-%d", i)),
	//	}
	//	// Attempt to send the message asynchronously and handle the response
	//	producer.SendAsync(ctx, asyncMsg, func(msg pulsar.ProducerMessage, err error) {
	//		if err != nil {
	//			log.Fatal(err)
	//		}
	//	})
	//	producer.Flush()
	//}
}

