package main

import (
	"log"
	"os"

	"github.com/apache/pulsar/pulsar-client-go/pulsar"
)

type testJson struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

var exampleSchemaDef = "{\"type\":\"record\",\"name\":\"Example\",\"namespace\":\"test\"," +
	"\"fields\":[{\"name\":\"ID\",\"type\":\"int\"},{\"name\":\"Name\",\"type\":\"string\"}]}"

var (
	serviceURL = os.Args[1]
	topicName  = os.Args[2]
)

func createClient() pulsar.Client {
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: serviceURL,
	})
	if err != nil {
		log.Fatal(err)
	}
	return client
}

