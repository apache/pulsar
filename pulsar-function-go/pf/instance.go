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
	"fmt"
	"time"

	"github.com/apache/pulsar/pulsar-client-go/pulsar"
	"github.com/apache/pulsar/pulsar-function-go/log"
	"github.com/apache/pulsar/pulsar-function-go/pb"
)

const PulsarServiceURL = "pulsar://localhost:6650"

type GoInstance struct {
	function  Function
	context   *FunctionContext
	producer  pulsar.Producer
	consumers []pulsar.Consumer
	client    pulsar.Client
	msg       []pulsar.Message
}

func NewGoInstance() *GoInstance {
	goInstance := &GoInstance{
		context: NewFuncContext(),
	}
	return goInstance
}

type Handler struct {
	handler Function
}

func (gi *GoInstance) handlerMsg() (output []byte, err error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fc := NewFuncContext()
	ctx = NewContext(ctx, fc)

	var msgInput []byte
	for _, msg := range gi.msg {
		ctx = context.WithValue(ctx, "current-msg", msg)
		ctx = context.WithValue(ctx, "current-topic", msg.Topic())
		msgInput = msg.Payload()
	}
	output, err = gi.function.Process(ctx, msgInput)
	if err != nil {
		log.Errorf("process function err:%v", err)
		return nil, err
	}
	return output, nil
}

func (gi *GoInstance) setupClient() {
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: PulsarServiceURL,
	})
	if err != nil {
		log.Errorf("create client error:%v", err)
		return
	}
	gi.client = client
}

func (gi *GoInstance) StartFunction(function Function) {
	// Setup consumers and input deserializers
	consumerType := pulsar.Shared
	gi.context = NewFuncContext()
	gi.setupClient()
	if int32(gi.context.InstanceConf.FuncDetails.Source.SubscriptionType) == pb.SubscriptionType_value["FAILOVER"] {
		consumerType = pulsar.Failover
	}

	funcDetails := gi.context.InstanceConf.FuncDetails
	subscriptionName := funcDetails.Tenant + "/" + funcDetails.Namespace + "/" + funcDetails.Name

	properties := getProperties(getDefaultSubscriptionName(
		funcDetails.Tenant,
		funcDetails.Namespace,
		funcDetails.Name), gi.context.InstanceConf.InstanceID)

	channel := make(chan pulsar.ConsumerMessage)

	for topic, consumerConf := range funcDetails.Source.InputSpecs {
		log.Debugf("Setting up consumer for topic: %s with subscription name: %s", topic, subscriptionName)
		if consumerConf.ReceiverQueueSize != nil {
			if consumerConf.IsRegexPattern {
				consumer, err := gi.client.Subscribe(pulsar.ConsumerOptions{
					TopicsPattern:     topic,
					ReceiverQueueSize: int(consumerConf.ReceiverQueueSize.Value),
					SubscriptionName:  subscriptionName,
					Properties:        properties,
					Type:              consumerType,
					MessageChannel:    channel,
				})
				if err != nil {
					log.Errorf("create consumer error:%s", err.Error())
				}
				gi.consumers = append(gi.consumers, consumer)
			} else {
				consumer, err := gi.client.Subscribe(pulsar.ConsumerOptions{
					Topic:             topic,
					SubscriptionName:  subscriptionName,
					Properties:        properties,
					Type:              consumerType,
					ReceiverQueueSize: int(consumerConf.ReceiverQueueSize.Value),
					MessageChannel:    channel,
				})
				if err != nil {
					log.Errorf("create consumer error:%s", err.Error())
				}

				gi.consumers = append(gi.consumers, consumer)
			}
		} else {
			if consumerConf.IsRegexPattern {
				consumer, err := gi.client.Subscribe(pulsar.ConsumerOptions{
					TopicsPattern:    topic,
					SubscriptionName: subscriptionName,
					Properties:       properties,
					Type:             consumerType,
					MessageChannel:   channel,
				})
				if err != nil {
					log.Errorf("create consumer error:%s", err.Error())
				}
				gi.consumers = append(gi.consumers, consumer)
			} else {
				consumer, err := gi.client.Subscribe(pulsar.ConsumerOptions{
					Topic:            topic,
					SubscriptionName: subscriptionName,
					Properties:       properties,
					Type:             consumerType,
					MessageChannel:   channel,
				})
				if err != nil {
					log.Errorf("create consumer error:%s", err.Error())
				}
				gi.consumers = append(gi.consumers, consumer)
			}
		}
		gi.context.InputTopics = append(gi.context.InputTopics, topic)

	CLOSE:
		for {
			select {
			case cm := <-channel:
				gi.msg = append(gi.msg, cm.Message)
			case <-time.After(time.Millisecond * 500):
				close(channel)
				break CLOSE
			}
		}
		fmt.Println("channel msg number is:", len(gi.msg))
	}

	gi.actualExecution(function)
	gi.close()
}

func (gi *GoInstance) actualExecution(function Function) {
	gi.function = function
	output, err := gi.handlerMsg()
	err = gi.processResult(output)
	if err != nil {
		log.Errorf("process output result err:%v", err)
		return
	}
}

func (gi *GoInstance) processResult(output []byte) error {
	gi.context = NewFuncContext()
	atLeastOnce := gi.context.InstanceConf.FuncDetails.ProcessingGuarantees == pb.ProcessingGuarantees_ATLEAST_ONCE

	if output != nil && gi.context.InstanceConf.FuncDetails.Sink.Topic != "" &&
		len(gi.context.InstanceConf.FuncDetails.Sink.Topic) > 0 {
		if gi.producer == nil {
			err := gi.setUpProducer()
			if err != nil {
				log.Errorf("set up producer error:%s", err.Error())
				return err
			}
		}

		asyncMsg := pulsar.ProducerMessage{
			Payload: output,
		}
		// Attempt to send the message asynchronously and handle the response
		gi.producer.SendAsync(context.Background(), asyncMsg, func(message pulsar.ProducerMessage, e error) {
			if e != nil {
				log.Fatal(e)
			}
		})
		fmt.Printf("Message [%s] successfully published\n", string(asyncMsg.Payload))

		for _, consumer := range gi.consumers {
			for _, msg := range gi.msg {
				gi.doneProduce(consumer, msg, gi.producer.Topic(), 0)

				if atLeastOnce && gi.context.InstanceConf.FuncDetails.AutoAck {
					err := consumer.Ack(msg)
					if err != nil {
						log.Errorf("ack msg error:%s", err.Error())
						return err
					}
				}
			}
		}
	}
	return nil
}

func (gi *GoInstance) doneProduce(consumer pulsar.Consumer, origMsg pulsar.Message, topic string, result pulsar.Result) {
	if result == 0 {
		gi.context = NewFuncContext()
		if gi.context.InstanceConf.FuncDetails.AutoAck {
			err := consumer.Ack(origMsg)
			if err != nil {
				log.Errorf("ack oriMsg error:%v", err)
			}
		}
	} else {
		log.Errorf("Failed to publish to topic [%s] with error [%s] with src message id [%s]",
			topic, result, origMsg.ID())
		return
	}
}

func (gi *GoInstance) setUpProducer() (err error) {
	gi.context = NewFuncContext()
	gi.setupClient()
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
