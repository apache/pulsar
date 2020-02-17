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

package pf

import (
	"context"
	"math"
	"time"

	"github.com/golang/protobuf/ptypes/empty"

	"github.com/apache/pulsar-client-go/pulsar"
	log "github.com/apache/pulsar/pulsar-function-go/logutil"
	pb "github.com/apache/pulsar/pulsar-function-go/pb"
)

type goInstance struct {
	function          function
	context           *FunctionContext
	producer          pulsar.Producer
	consumers         map[string]pulsar.Consumer
	client            pulsar.Client
	lastHealthCheckTs int64
	properties        map[string]string
}

// newGoInstance init goInstance and init function context
func newGoInstance() *goInstance {
	goInstance := &goInstance{
		context:   NewFuncContext(),
		consumers: make(map[string]pulsar.Consumer),
	}
	now := time.Now()
	goInstance.lastHealthCheckTs = now.UnixNano()
	goInstance.properties = make(map[string]string)
	return goInstance
}

func (gi *goInstance) processSpawnerHealthCheckTimer(tkr *time.Ticker) {
	log.Info("Starting processSpawnerHealthCheckTimer")
	now := time.Now()
	maxIdleTime := gi.context.GetMaxIdleTime()
	timeSinceLastCheck := now.UnixNano() - gi.lastHealthCheckTs
	if (timeSinceLastCheck) > (maxIdleTime) {
		log.Error("Haven't received health check from spawner in a while. Stopping instance...")
		gi.close()
		tkr.Stop()
	}
}

func (gi *goInstance) startScheduler() {
	if gi.context.instanceConf.expectedHealthCheckInterval > 0 {
		log.Info("Starting Scheduler")
		go func() {
			log.Info("Started Scheduler")
			tkr := time.NewTicker(time.Millisecond * 1000 * gi.context.GetExpectedHealthCheckIntervalAsDuration())
			for range tkr.C {
				log.Info("Starting Timer")
				go gi.processSpawnerHealthCheckTimer(tkr)
			}
		}()
	}
}

func (gi *goInstance) startFunction(function function) error {
	gi.function = function

	// start process spawner health check timer
	now := time.Now()
	gi.lastHealthCheckTs = now.UnixNano()

	gi.startScheduler()

	err := gi.setupClient()
	if err != nil {
		log.Errorf("setup client failed, error is:%v", err)
		return err
	}
	err = gi.setupProducer()
	if err != nil {
		log.Errorf("setup producer failed, error is:%v", err)
		return err
	}
	channel, err := gi.setupConsumer()
	if err != nil {
		log.Errorf("setup consumer failed, error is:%v", err)
		return err
	}
	err = gi.setupLogHandler()
	if err != nil {
		log.Errorf("setup log appender failed, error is:%v", err)
		return err
	}

	idleDuration := getIdleTimeout(time.Millisecond * gi.context.instanceConf.killAfterIdle)
	idleTimer := time.NewTimer(idleDuration)
	defer idleTimer.Stop()

	servicer := InstanceControlServicer{goInstance: gi}
	servicer.serve(gi)

CLOSE:
	for {
		idleTimer.Reset(idleDuration)
		select {
		case cm := <-channel:
			msgInput := cm.Message
			atMostOnce := gi.context.instanceConf.funcDetails.ProcessingGuarantees == pb.ProcessingGuarantees_ATMOST_ONCE
			atLeastOnce := gi.context.instanceConf.funcDetails.ProcessingGuarantees == pb.ProcessingGuarantees_ATLEAST_ONCE
			autoAck := gi.context.instanceConf.funcDetails.AutoAck
			if autoAck && atMostOnce {
				gi.ackInputMessage(msgInput)
			}

			gi.addLogTopicHandler()

			output, err := gi.handlerMsg(msgInput)
			if err != nil {
				log.Errorf("handler message error:%v", err)
				if autoAck && atLeastOnce {
					gi.nackInputMessage(msgInput)
				}
				return err
			}

			gi.processResult(msgInput, output)

		case <-idleTimer.C:
			close(channel)
			break CLOSE
		}
	}

	gi.closeLogTopic()
	gi.close()
	return nil
}

func (gi *goInstance) setupClient() error {
	client, err := pulsar.NewClient(pulsar.ClientOptions{

		URL: gi.context.instanceConf.pulsarServiceURL,
	})
	if err != nil {
		log.Errorf("create client error:%v", err)
		return err
	}
	gi.client = client
	return nil
}

func (gi *goInstance) setupProducer() (err error) {
	if gi.context.instanceConf.funcDetails.Sink.Topic != "" && len(gi.context.instanceConf.funcDetails.Sink.Topic) > 0 {
		log.Debugf("Setting up producer for topic %s", gi.context.instanceConf.funcDetails.Sink.Topic)
		properties := getProperties(getDefaultSubscriptionName(
			gi.context.instanceConf.funcDetails.Tenant,
			gi.context.instanceConf.funcDetails.Namespace,
			gi.context.instanceConf.funcDetails.Name), gi.context.instanceConf.instanceID)
		gi.producer, err = gi.client.CreateProducer(pulsar.ProducerOptions{
			Topic:                   gi.context.instanceConf.funcDetails.Sink.Topic,
			Properties:              properties,
			CompressionType:         pulsar.LZ4,
			BatchingMaxPublishDelay: time.Millisecond * 10,
			// Set send timeout to be infinity to prevent potential deadlock with consumer
			// that might happen when consumer is blocked due to unacked messages
		})
		if err != nil {
			log.Errorf("create producer error:%s", err.Error())
			return err
		}
	}
	return nil
}

func (gi *goInstance) setupConsumer() (chan pulsar.ConsumerMessage, error) {
	subscriptionType := pulsar.Shared
	if int32(gi.context.instanceConf.funcDetails.Source.SubscriptionType) == pb.SubscriptionType_value["FAILOVER"] {
		subscriptionType = pulsar.Failover
	}

	funcDetails := gi.context.instanceConf.funcDetails
	subscriptionName := funcDetails.Tenant + "/" + funcDetails.Namespace + "/" + funcDetails.Name

	properties := getProperties(getDefaultSubscriptionName(
		funcDetails.Tenant,
		funcDetails.Namespace,
		funcDetails.Name), gi.context.instanceConf.instanceID)

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
			return nil, err
		}
		gi.consumers[topic] = consumer
	}
	return channel, nil
}

func (gi *goInstance) handlerMsg(input pulsar.Message) (output []byte, err error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = NewContext(ctx, gi.context)
	msgInput := input.Payload()
	return gi.function.process(ctx, msgInput)
}

func (gi *goInstance) processResult(msgInput pulsar.Message, output []byte) {
	atLeastOnce := gi.context.instanceConf.funcDetails.ProcessingGuarantees == pb.ProcessingGuarantees_ATLEAST_ONCE
	atMostOnce := gi.context.instanceConf.funcDetails.ProcessingGuarantees == pb.ProcessingGuarantees_ATMOST_ONCE
	autoAck := gi.context.instanceConf.funcDetails.AutoAck

	if output != nil && gi.context.instanceConf.funcDetails.Sink.Topic != "" {
		asyncMsg := pulsar.ProducerMessage{
			Payload: output,
		}
		// Attempt to send the message and handle the response
		gi.producer.SendAsync(context.Background(), &asyncMsg, func(messageID pulsar.MessageID,
			message *pulsar.ProducerMessage, err error) {
			if err != nil {
				if autoAck && atLeastOnce {
					gi.nackInputMessage(msgInput)
				}
				log.Fatal(err)
			} else if autoAck && !atMostOnce {
				gi.ackInputMessage(msgInput)
			}
		})
	} else if autoAck && atLeastOnce {
		gi.ackInputMessage(msgInput)
	}
}

// ackInputMessage doesn't produce any result, or the user doesn't want the result.
func (gi *goInstance) ackInputMessage(inputMessage pulsar.Message) {
	gi.consumers[inputMessage.Topic()].Ack(inputMessage)
}

func (gi *goInstance) nackInputMessage(inputMessage pulsar.Message) {
	gi.consumers[inputMessage.Topic()].Nack(inputMessage)
}

func getIdleTimeout(timeoutMilliSecond time.Duration) time.Duration {
	if timeoutMilliSecond <= 0 {
		return time.Duration(math.MaxInt64)
	}
	return timeoutMilliSecond
}

func (gi *goInstance) setupLogHandler() error {
	if gi.context.instanceConf.funcDetails.GetLogTopic() != "" {
		gi.context.logAppender = NewLogAppender(
			gi.client, //pulsar client
			gi.context.instanceConf.funcDetails.GetLogTopic(), //log topic
			getDefaultSubscriptionName(gi.context.instanceConf.funcDetails.Tenant, //fqn
				gi.context.instanceConf.funcDetails.Namespace,
				gi.context.instanceConf.funcDetails.Name),
		)
		return gi.context.logAppender.Start()
	}
	return nil
}

func (gi *goInstance) addLogTopicHandler() {
	// Clear StrEntry regardless gi.context.logAppender is set or not
	defer func() {
		log.StrEntry = nil
	}()

	if gi.context.logAppender == nil {
		log.Error("the logAppender is nil, if you want to use it, please specify `--log-topic` at startup.")
		return
	}

	for _, logByte := range log.StrEntry {
		gi.context.logAppender.Append([]byte(logByte))
	}
}

func (gi *goInstance) closeLogTopic() {
	log.Info("closing log topic...")
	if gi.context.logAppender == nil {
		return
	}
	gi.context.logAppender.Stop()
	gi.context.logAppender = nil
}

func (gi *goInstance) close() {
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

func (gi *goInstance) healthCheck() *pb.HealthCheckResult {
	now := time.Now()
	gi.lastHealthCheckTs = now.UnixNano()
	healthCheckResult := pb.HealthCheckResult{Success: true}
	return &healthCheckResult
}

func (gi *goInstance) getFunctionStatus() *pb.FunctionStatus {
	return nil // Not implemented until we add the statistics features
}

func (gi *goInstance) getAndResetMetrics() *pb.MetricsData {
	return nil // Not implemented until we add the statistics features
}

func (gi *goInstance) resetMetrics() *empty.Empty {
	return nil // Not implemented until we add the statistics features
}

func (gi *goInstance) getMetrics() *pb.MetricsData {
	return nil // Not implemented until we add the statistics features
}
