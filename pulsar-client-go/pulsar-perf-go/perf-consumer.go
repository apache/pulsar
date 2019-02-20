//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/apache/pulsar/pulsar-client-go/pulsar"
	"github.com/spf13/cobra"
	"log"
	"sync/atomic"
	"time"
)

type ConsumeArgs struct {
	Topic             string
	SubscriptionName  string
	ReceiverQueueSize int
}

var consumeArgs ConsumeArgs

var cmdConsume = &cobra.Command{
	Use:   "consume <topic>",
	Short: "Consume from topic",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		consumeArgs.Topic = args[0]
		consume()
	},
}

func initConsumer() {
	cmdConsume.Flags().StringVarP(&consumeArgs.SubscriptionName, "subscription", "s", "sub", "Subscription name")
	cmdConsume.Flags().IntVarP(&consumeArgs.ReceiverQueueSize, "receiver-queue-size", "r", 1000, "Receiver queue size")
}

func consume() {
	b, _ := json.MarshalIndent(clientArgs, "", "  ")
	fmt.Println("Client config: ", string(b))
	b, _ = json.MarshalIndent(consumeArgs, "", "  ")
	fmt.Println("Consumer config: ", string(b))

	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:                    clientArgs.ServiceUrl,
		IOThreads:              clientArgs.IoThreads,
		StatsIntervalInSeconds: 0,
	})

	if err != nil {
		log.Fatal(err)
	}

	defer client.Close()

	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            consumeArgs.Topic,
		SubscriptionName: consumeArgs.SubscriptionName,
	})

	if err != nil {
		log.Fatal(err)
	}

	defer consumer.Close()

	ctx := context.Background()

	var msgReceived int64 = 0
	var bytesReceived int64 = 0

	go func() {
		for {
			msg, _ := consumer.Receive(ctx)

			atomic.AddInt64(&msgReceived, 1)
			atomic.AddInt64(&bytesReceived, int64(len(msg.Payload())))

			consumer.Ack(msg)
		}
	}()

	// Print stats of the consume rate
	tick := time.NewTicker(10 * time.Second)

	for {
		select {
		case <-tick.C:
			currentMsgReceived := atomic.SwapInt64(&msgReceived, 0)
			currentBytesReceived := atomic.SwapInt64(&bytesReceived, 0)
			msgRate := float64(currentMsgReceived) / float64(10)
			bytesRate := float64(currentBytesReceived) / float64(10)

			log.Printf(`Stats - Consume rate: %6.1f msg/s - %6.1f Mbps`,
				msgRate, bytesRate*8/1024/1024)
		}
	}
}
