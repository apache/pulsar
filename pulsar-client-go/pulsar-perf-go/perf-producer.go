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
	"github.com/bmizerany/perks/quantile"
	"github.com/spf13/cobra"
	"github.com/beefsack/go-rate"
	"log"
	"time"
)

type ProduceArgs struct {
	Topic             string
	Rate              int
	Batching          bool
	MessageSize       int
	ProducerQueueSize int
}

var produceArgs ProduceArgs

var cmdProduce = &cobra.Command{
	Use:   "produce ",
	Short: "Produce on a topic and measure performance",
	Args: cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		produceArgs.Topic = args[0]
		produce()
	},
}

func initProducer() {
	cmdProduce.Flags().IntVarP(&produceArgs.Rate, "rate", "r", 100, "Publish rate. Set to 0 to go unthrottled")
	cmdProduce.Flags().BoolVarP(&produceArgs.Batching, "batching", "b", true, "Enable batching")
	cmdProduce.Flags().IntVarP(&produceArgs.MessageSize, "size", "s", 1024, "Message size")
	cmdProduce.Flags().IntVarP(&produceArgs.ProducerQueueSize, "queue-size", "q", 1000, "Produce queue size")
}

func produce() {
	b, _ := json.MarshalIndent(clientArgs, "", "  ")
	fmt.Println("Client config: ", string(b))
	b, _ = json.MarshalIndent(produceArgs, "", "  ")
	fmt.Println("Producer config: ", string(b))

	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:                    clientArgs.ServiceUrl,
		IOThreads:              clientArgs.IoThreads,
		StatsIntervalInSeconds: 0,
	})

	if err != nil {
		log.Fatal(err)
	}

	defer client.Close()

	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic:                   produceArgs.Topic,
		MaxPendingMessages:      produceArgs.ProducerQueueSize,
		Batching:                produceArgs.Batching,
		BatchingMaxPublishDelay: 1 * time.Millisecond,
		SendTimeout:             0,
		BlockIfQueueFull:        true,
	})
	if err != nil {
		log.Fatal(err)
	}

	defer producer.Close()

	ctx := context.Background()

	payload := make([]byte, produceArgs.MessageSize)

	ch := make(chan float64)

	go func() {
		var rateLimiter *rate.RateLimiter = nil
		if produceArgs.Rate > 0 {
			rateLimiter = rate.New(produceArgs.Rate, time.Second)
		}

		for {
			if rateLimiter != nil {
				rateLimiter.Wait()
			}

			start := time.Now()

			producer.SendAsync(ctx, pulsar.ProducerMessage{
				Payload: payload,
			}, func(message pulsar.ProducerMessage, e error) {
				if e != nil {
					log.Fatal("Failed to publish", e)
				}

				latency := time.Since(start).Seconds()
				ch <- latency
			})
		}
	}()

	// Print stats of the publish rate and latencies
	tick := time.NewTicker(10 * time.Second)
	q := quantile.NewTargeted(0.90, 0.95, 0.99, 0.999, 1.0)
	messagesPublished := 0

	for {
		select {
		case <-tick.C:
			messageRate := float64(messagesPublished) / float64(10)
			log.Printf(`Stats - Publish rate: %6.1f msg/s - %6.1f Mbps - Latency ms: 50%% %5.1f - 95%% %5.1f - 99%% %5.1f - 99.9%% %5.1f - max %6.1f`,
				messageRate,
				messageRate*float64(produceArgs.MessageSize)/1024/1024*8,
				q.Query(0.5)*1000,
				q.Query(0.95)*1000,
				q.Query(0.99)*1000,
				q.Query(0.999)*1000,
				q.Query(1.0)*1000,
			)

			q.Reset()
			messagesPublished = 0
		case latency := <-ch:
			messagesPublished += 1
			q.Insert(latency)
		}
	}
}
