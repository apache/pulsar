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
package instance

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
	msg       pulsar.Message
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

func (hd *Handler) handlerMsg() ([]byte, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	gi := NewGoInstance()
	fc := NewFuncContext()
	ctx = NewContext(ctx, fc)
	ctx = context.WithValue(ctx, "current-msg", gi.msg)
	fmt.Println(gi.msg)
	ctx = context.WithValue(ctx, "current-topic", gi.msg.Topic())
	msgInput := gi.msg.Payload()
	output, err := gi.function.Process(ctx, msgInput)
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

	properties := GetProperties(GetDefaultSubscriptionName(
		funcDetails.Tenant,
		funcDetails.Namespace,
		funcDetails.Name), gi.context.InstanceConf.InstanceID)

	//for topic := range funcDetails.Source.TopicsToSerDeClassName {
	//
	//	log.Debugf("Setting up consumer for topic: %s with subscription name: %s", topic, subscriptionName)
	//	consumer, err := gi.client.Subscribe(pulsar.ConsumerOptions{
	//		Topic:            topic,
	//		SubscriptionName: subscriptionName,
	//		Type:             consumerType,
	//		Properties:       properties,
	//	})
	//	if err != nil {
	//		log.Errorf("create consumer error:%s", err.Error())
	//	}
	//	gi.consumers = append(gi.consumers, consumer)
	//	gi.context.InputTopics = append(gi.context.InputTopics, topic)
	//}

	channel := make(chan pulsar.ConsumerMessage)
	for topic, consumerConf := range funcDetails.Source.InputSpecs {
		log.Debugf("Setting up consumer for topic: %s with subscription name: %s", topic, subscriptionName)
		if consumerConf.ReceiverQueueSize != nil {
			if consumerConf.IsRegexPattern {
				consumer, err := gi.client.Subscribe(pulsar.ConsumerOptions{
					TopicsPattern:     topic,
					SubscriptionName:  subscriptionName,
					Properties:        properties,
					Type:              consumerType,
					ReceiverQueueSize: int(consumerConf.ReceiverQueueSize.Value),
					MessageChannel:    channel,
				})
				if err != nil {
					log.Errorf("create consumer error:%s", err.Error())
				}
				for cm := range channel {
					gi.msg = cm.Message
				}
				gi.consumers = append(gi.consumers, consumer)
				gi.context.InputTopics = append(gi.context.InputTopics, topic)
			} else {
				fmt.Println(topic)
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

				for cm := range channel {
					gi.msg = cm.Message
					fmt.Println(gi.msg.Topic())
					fmt.Println(gi.msg.Payload())
					goto CLOSE
				}
				gi.consumers = append(gi.consumers, consumer)
				gi.context.InputTopics = append(gi.context.InputTopics, topic)
			}
		} else {
			if consumerConf.IsRegexPattern {
				fmt.Println(topic)
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
				for cm := range channel {
					gi.msg = cm.Message
				}
				gi.consumers = append(gi.consumers, consumer)
				gi.context.InputTopics = append(gi.context.InputTopics, topic)
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
				for cm := range channel {
					gi.msg = cm.Message
				}
				gi.consumers = append(gi.consumers, consumer)
				gi.context.InputTopics = append(gi.context.InputTopics, topic)
			}
		}
	}
CLOSE:
	close(channel)

	// Now launch a gorutine that does execution
	go func() {
		gi.actualExecution(function)
	}()
}

func (gi *GoInstance) actualExecution(function Function) {
	log.Debug("Started gorutine for executing the function")
	for {
		log.Debugf("Got a message from topic %s", gi.msg.Topic())
		fmt.Printf("Got a message from topic %s\n", gi.msg.Topic())
		fmt.Println("hello function")
		hd := new(Handler)
		hd.handler = function
		fmt.Println(hd.handler)
		output, err := hd.handlerMsg()
		err = gi.processResult(output)
		if err != nil {
			log.Errorf("process output result err:%v", err)
			return
		}
	}
}

func (gi *GoInstance) processResult(output []byte) error {
	gi.context = NewFuncContext()
	atLeastOnce := gi.context.InstanceConf.FuncDetails.ProcessingGuarantees == pb.ProcessingGuarantees_ATLEAST_ONCE

	for _, consumer := range gi.consumers {
		if output != nil && gi.context.InstanceConf.FuncDetails.Sink.Topic != "" &&
			len(gi.context.InstanceConf.FuncDetails.Sink.Topic) > 0 {
			if output != nil {
				output = gi.msg.Payload()
			}
			if gi.producer == nil {
				err := gi.setUpProducer()
				if err != nil {
					log.Errorf("set up producer error:%s", err.Error())
					return err
				}
			}
			//serialize function output
			if output != nil {
				asyncMsg := pulsar.ProducerMessage{
					Payload: output,
				}
				// Attempt to send the message asynchronously and handle the response
				gi.producer.SendAsync(context.Background(), asyncMsg, func(msg pulsar.ProducerMessage, err error) {
					if err != nil {
						log.Fatal(err)
					}
					fmt.Printf("Message %s successfully published", msg.Payload)
				})

				gi.doneProduce(consumer, gi.msg, gi.producer.Topic(), 1)
			}

		} else if atLeastOnce && gi.context.InstanceConf.FuncDetails.AutoAck {
			err := consumer.Ack(gi.msg)
			log.Errorf("ack msg error:%s", err.Error())
			return err
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
		properties := GetProperties(GetDefaultSubscriptionName(
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

func (gi *GoInstance) Close() {
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

//
//func (gi *GoInstance) setupLogHandler() {
//	if gi.conf.FuncDetails.GetLogTopic() != "" {
//		gi.logAppender = NewLogAppender(
//			gi.client,                         //pulsar client
//			gi.conf.FuncDetails.GetLogTopic(), //log topic
//			GetDefaultSubscriptionName(gi.conf.FuncDetails.Tenant, //fqn
//				gi.conf.FuncDetails.Namespace,
//				gi.conf.FuncDetails.Name),
//		)
//		gi.logAppender.Start()
//	}
//}
//
//func (gi *GoInstance) addLogTopicHandler() {
//	if gi.logAppender == nil {
//		return
//	}
//
//	log.SetOutputByName(gi.logAppender.GetName())
//
//	//param: add logbyte []byte
//	formatter := &log.TextFormatter{}
//	logbyte, err := formatter.Format(&logrus.Entry{})
//	if err != nil {
//		log.Errorf("formatter error:%s", err.Error())
//	}
//	gi.logAppender.Append(logbyte)
//}
//
//func (gi *GoInstance) removeLogTopicHandler() {
//	if gi.logAppender == nil {
//		return
//	}
//
//	log.Close()
//}
