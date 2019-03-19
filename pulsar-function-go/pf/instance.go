/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package pf

import (
	"context"
	"time"

	"github.com/apache/pulsar/pulsar-client-go/pulsar"
	"github.com/apache/pulsar/pulsar-function-go/log"
	"github.com/apache/pulsar/pulsar-function-go/pb"
)

type GoInstance struct {
	function  Function
	context   *FunctionContext
	producer  pulsar.Producer
	consumers map[string]pulsar.Consumer
	client    pulsar.Client
}

// NewGoInstance init GoInstance and init function context
func NewGoInstance() *GoInstance {
	goInstance := &GoInstance{
		context:   NewFuncContext(),
		consumers: make(map[string]pulsar.Consumer),
	}
	return goInstance
}

type Handler struct {
	handler Function
}

func (gi *GoInstance) StartFunction(function Function) {
	gi.function = function
	gi.setupClient()
	err := gi.setupProducer()
	if err != nil {
		log.Errorf("setup producer failed, error is:%s", err.Error())
	}
	channel := gi.setupConsumer()

CLOSE:
	for {
		select {
		case cm := <-channel:
			msgInput := cm.Message
			output, err := gi.handlerMsg(msgInput)
			if err != nil {
				log.Errorf("handler message error:%v", err)
			}
			err = gi.processResult(msgInput, output)
			if err != nil {
				log.Errorf("process result error:%v", err)
			}
		case <-time.After(time.Millisecond * gi.context.InstanceConf.KillAfterIdleMs):
			close(channel)
			break CLOSE
		}
	}

	gi.close()
}

func (gi *GoInstance) setupClient() {
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: gi.context.InstanceConf.PulsarServiceURL,
	})
	if err != nil {
		log.Errorf("create client error:%v", err)
		return
	}
	gi.client = client
}

func (gi *GoInstance) setupProducer() (err error) {
	if gi.client == nil {
		gi.setupClient()
	}

	if gi.context.InstanceConf.FuncDetails.Sink.Topic != "" && len(gi.context.InstanceConf.FuncDetails.Sink.Topic) > 0 {
		log.Debugf("Setting up producer for topic %s", gi.context.InstanceConf.FuncDetails.Sink.Topic)
		properties := getProperties(getDefaultSubscriptionName(
			gi.context.InstanceConf.FuncDetails.Tenant,
			gi.context.InstanceConf.FuncDetails.Namespace,
			gi.context.InstanceConf.FuncDetails.Name), gi.context.InstanceConf.InstanceID)
		gi.producer, err = gi.client.CreateProducer(pulsar.ProducerOptions{
			Topic:                   gi.context.InstanceConf.FuncDetails.Sink.Topic,
			Properties:              properties,
			CompressionType:         pulsar.LZ4,
			BlockIfQueueFull:        true,
			Batching:                true,
			BatchingMaxPublishDelay: time.Millisecond * 10,
			// set send timeout to be infinity to prevent potential deadlock with consumer
			// that might happen when consumer is blocked due to unacked messages
			SendTimeout: 0,
		})
		if err != nil {
			log.Errorf("create producer error:%s", err.Error())
		}
		return err
	}
	return nil
}

func (gi *GoInstance) setupConsumer() chan pulsar.ConsumerMessage {
	subscriptionType := pulsar.Shared
	if int32(gi.context.InstanceConf.FuncDetails.Source.SubscriptionType) == pb.SubscriptionType_value["FAILOVER"] {
		subscriptionType = pulsar.Failover
	}

	funcDetails := gi.context.InstanceConf.FuncDetails
	subscriptionName := funcDetails.Tenant + "/" + funcDetails.Namespace + "/" + funcDetails.Name

	properties := getProperties(getDefaultSubscriptionName(
		funcDetails.Tenant,
		funcDetails.Namespace,
		funcDetails.Name), gi.context.InstanceConf.InstanceID)

	channel := make(chan pulsar.ConsumerMessage)

	var (
		consumer pulsar.Consumer
		err      error
	)

	for topic, consumerConf := range funcDetails.Source.InputSpecs {
		log.Debugf("Setting up consumer for topic: %s with subscription name: %s", topic, subscriptionName)
		if consumerConf.ReceiverQueueSize != nil {
			if consumerConf.IsRegexPattern {
				consumer, err = gi.client.Subscribe(pulsar.ConsumerOptions{
					TopicsPattern:     topic,
					ReceiverQueueSize: int(consumerConf.ReceiverQueueSize.Value),
					SubscriptionName:  subscriptionName,
					Properties:        properties,
					Type:              subscriptionType,
					MessageChannel:    channel,
				})
			} else {
				consumer, err = gi.client.Subscribe(pulsar.ConsumerOptions{
					Topic:             topic,
					SubscriptionName:  subscriptionName,
					Properties:        properties,
					Type:              subscriptionType,
					ReceiverQueueSize: int(consumerConf.ReceiverQueueSize.Value),
					MessageChannel:    channel,
				})
			}
		} else {
			if consumerConf.IsRegexPattern {
				consumer, err = gi.client.Subscribe(pulsar.ConsumerOptions{
					TopicsPattern:    topic,
					SubscriptionName: subscriptionName,
					Properties:       properties,
					Type:             subscriptionType,
					MessageChannel:   channel,
				})
			} else {
				consumer, err = gi.client.Subscribe(pulsar.ConsumerOptions{
					Topic:            topic,
					SubscriptionName: subscriptionName,
					Properties:       properties,
					Type:             subscriptionType,
					MessageChannel:   channel,
				})

			}
		}

		if err != nil {
			log.Errorf("create consumer error:%s", err.Error())
		}
		gi.consumers[topic] = consumer
	}
	return channel
}

func (gi *GoInstance) handlerMsg(input pulsar.Message) (output []byte, err error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = NewContext(ctx, gi.context)
	msgInput := input.Payload()
	output, err = gi.function.Process(ctx, msgInput)
	if err != nil {
		log.Errorf("process function err:%v", err)
		return nil, err
	}

	return output, nil
}

func (gi *GoInstance) processResult(msgInput pulsar.Message, output []byte) error {
	atMostOnce := gi.context.InstanceConf.FuncDetails.ProcessingGuarantees == pb.ProcessingGuarantees_ATMOST_ONCE
	atLeastOnce := gi.context.InstanceConf.FuncDetails.ProcessingGuarantees == pb.ProcessingGuarantees_ATLEAST_ONCE
	autoAck := gi.context.InstanceConf.FuncDetails.AutoAck

	if atMostOnce && autoAck {
		for topic, consumer := range gi.consumers {
			err := consumer.Ack(msgInput)
			if err != nil {
				log.Errorf("[at most once] ack input message error:%v, topic is:[%s]", err, topic)
				return err
			}
		}
	}

	if output != nil && gi.context.InstanceConf.FuncDetails.Sink.Topic != "" {
		asyncMsg := pulsar.ProducerMessage{
			Payload: output,
		}
		// Attempt to send the message asynchronously and handle the response
		gi.producer.SendAsync(context.Background(), asyncMsg, func(message pulsar.ProducerMessage, e error) {
			if e != nil {
				log.Fatal(e)
			}

			if atLeastOnce && autoAck {
				for topic, consumer := range gi.consumers {
					err := consumer.Ack(msgInput)
					if err != nil {
						log.Errorf("[at least once] ack input message error:%v, topic is:[%s]", err, topic)
					}
				}
			}
		})
	}
	return nil
}

func (gi *GoInstance) close() {
	log.Info("closing go instance...")
	if gi.producer != nil {
		gi.producer.Close()
	}
	if gi.consumers != nil {
		for _, consumer := range gi.consumers {
			consumer.Close()
		}
	}
	if gi.client != nil {
		gi.client.Close()
	}
}
