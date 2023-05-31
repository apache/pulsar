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
	"strconv"
	"time"

	"github.com/golang/protobuf/ptypes/empty"

	"github.com/apache/pulsar-client-go/pulsar"

	log "github.com/apache/pulsar/pulsar-function-go/logutil"
	pb "github.com/apache/pulsar/pulsar-function-go/pb"
	prometheus_client "github.com/prometheus/client_model/go"
)

type goInstance struct {
	function          function
	context           *FunctionContext
	producer          pulsar.Producer
	consumers         map[string]pulsar.Consumer
	client            pulsar.Client
	lastHealthCheckTs int64
	properties        map[string]string
	stats             StatWithLabelValues
}

func (gi *goInstance) getMetricsLabels() []string {
	// e.g. metrics_labels = []string{"test-tenant","test-tenant/test-namespace", "test-name", "1234", "test-cluster",
	//	"test-tenant/test-namespace/test-name"}
	metricsLabels := []string{
		gi.context.GetFuncTenant(),
		gi.context.GetTenantAndNamespace(),
		gi.context.GetFuncName(),
		gi.context.GetFuncID(),
		gi.context.GetClusterName(),
		gi.context.GetTenantAndNamespaceAndName(),
	}
	return metricsLabels
}

// newGoInstance init goInstance and init function context
func newGoInstance() *goInstance {
	goInstance := &goInstance{
		context:   NewFuncContext(),
		consumers: make(map[string]pulsar.Consumer),
	}
	now := time.Now()

	goInstance.context.outputMessage = func(topic string) pulsar.Producer {
		producer, err := goInstance.getProducer(topic)
		if err != nil {
			log.Fatal(err)
		}
		return producer
	}

	goInstance.lastHealthCheckTs = now.UnixNano()
	goInstance.properties = make(map[string]string)
	goInstance.stats = NewStatWithLabelValues(goInstance.getMetricsLabels()...)
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

	metricsServicer := NewMetricsServicer(gi)
	metricsServicer.serve()
	defer metricsServicer.close()
CLOSE:
	for {
		select {
		case cm := <-channel:
			msgInput := cm.Message
			atMostOnce := gi.context.instanceConf.funcDetails.ProcessingGuarantees == pb.ProcessingGuarantees_ATMOST_ONCE
			atLeastOnce := gi.context.instanceConf.funcDetails.ProcessingGuarantees == pb.ProcessingGuarantees_ATLEAST_ONCE
			autoAck := gi.context.instanceConf.funcDetails.AutoAck
			if autoAck && atMostOnce {
				gi.ackInputMessage(msgInput)
			}
			gi.stats.incrTotalReceived()
			gi.addLogTopicHandler()

			gi.stats.setLastInvocation()
			gi.stats.processTimeStart()

			output, err := gi.handlerMsg(msgInput)
			if err != nil {
				log.Errorf("handler message error:%v", err)
				if autoAck && atLeastOnce {
					gi.nackInputMessage(msgInput)
				}
				gi.stats.incrTotalUserExceptions(err)
				return err
			}

			gi.stats.processTimeEnd()
			gi.processResult(msgInput, output)
			gi.stats.incrTotalProcessedSuccessfully()
		case <-idleTimer.C:
			close(channel)
			break CLOSE
		}
		// reset the idle timer and drain if appropriate before the next loop
		if !idleTimer.Stop() {
			<-idleTimer.C
		}
		idleTimer.Reset(idleDuration)
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
		gi.stats.incrTotalSysExceptions(err)
		return err
	}
	gi.client = client
	return nil
}

func (gi *goInstance) setupProducer() error {
	if gi.context.instanceConf.funcDetails.Sink.Topic != "" && len(gi.context.instanceConf.funcDetails.Sink.Topic) > 0 {
		log.Debugf("Setting up producer for topic %s", gi.context.instanceConf.funcDetails.Sink.Topic)
		producer, err := gi.getProducer(gi.context.instanceConf.funcDetails.Sink.Topic)
		if err != nil {
			log.Fatal(err)
		}

		gi.producer = producer
		return nil
	}

	return nil
}

func (gi *goInstance) getProducer(topicName string) (pulsar.Producer, error) {
	properties := getProperties(getDefaultSubscriptionName(
		gi.context.instanceConf.funcDetails.Tenant,
		gi.context.instanceConf.funcDetails.Namespace,
		gi.context.instanceConf.funcDetails.Name), gi.context.instanceConf.instanceID)

	batchBuilderType := pulsar.DefaultBatchBuilder

	if gi.context.instanceConf.funcDetails.Sink.ProducerSpec != nil {
		batchBuilder := gi.context.instanceConf.funcDetails.Sink.ProducerSpec.BatchBuilder
		if batchBuilder != "" {
			if batchBuilder == "KEY_BASED" {
				batchBuilderType = pulsar.KeyBasedBatchBuilder
			}
		}
	}

	producer, err := gi.client.CreateProducer(pulsar.ProducerOptions{
		Topic:                   topicName,
		Properties:              properties,
		CompressionType:         pulsar.LZ4,
		BatchingMaxPublishDelay: time.Millisecond * 10,
		BatcherBuilderType:      batchBuilderType,
		SendTimeout:             0,
		// Set send timeout to be infinity to prevent potential deadlock with consumer
		// that might happen when consumer is blocked due to unacked messages
	})
	if err != nil {
		gi.stats.incrTotalSysExceptions(err)
		log.Errorf("create producer error:%s", err.Error())
		return nil, err
	}

	return producer, err
}

func (gi *goInstance) setupConsumer() (chan pulsar.ConsumerMessage, error) {
	subscriptionType := pulsar.Shared
	if int32(gi.context.instanceConf.funcDetails.Source.SubscriptionType) == pb.SubscriptionType_value["FAILOVER"] {
		subscriptionType = pulsar.Failover
	}

	funcDetails := gi.context.instanceConf.funcDetails
	subscriptionName := funcDetails.Tenant + "/" + funcDetails.Namespace + "/" + funcDetails.Name
	if funcDetails.Source != nil && funcDetails.Source.SubscriptionName != "" {
		subscriptionName = funcDetails.Source.SubscriptionName
	}

	properties := getProperties(getDefaultSubscriptionName(
		funcDetails.Tenant,
		funcDetails.Namespace,
		funcDetails.Name), gi.context.instanceConf.instanceID)

	channel := make(chan pulsar.ConsumerMessage)

	var (
		consumer  pulsar.Consumer
		topicName *TopicName
		err       error
	)

	for topic, consumerConf := range funcDetails.Source.InputSpecs {
		topicName, err = ParseTopicName(topic)
		if err != nil {
			return nil, err
		}

		log.Debugf("Setting up consumer for topic: %s with subscription name: %s", topicName.Name, subscriptionName)
		if consumerConf.ReceiverQueueSize != nil {
			if consumerConf.IsRegexPattern {
				consumer, err = gi.client.Subscribe(pulsar.ConsumerOptions{
					TopicsPattern:     topicName.Name,
					ReceiverQueueSize: int(consumerConf.ReceiverQueueSize.Value),
					SubscriptionName:  subscriptionName,
					Properties:        properties,
					Type:              subscriptionType,
					MessageChannel:    channel,
				})
			} else {
				consumer, err = gi.client.Subscribe(pulsar.ConsumerOptions{
					Topic:             topicName.Name,
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
					TopicsPattern:    topicName.Name,
					SubscriptionName: subscriptionName,
					Properties:       properties,
					Type:             subscriptionType,
					MessageChannel:   channel,
				})
			} else {
				consumer, err = gi.client.Subscribe(pulsar.ConsumerOptions{
					Topic:            topicName.Name,
					SubscriptionName: subscriptionName,
					Properties:       properties,
					Type:             subscriptionType,
					MessageChannel:   channel,
				})

			}
		}

		if err != nil {
			log.Errorf("create consumer error:%s", err.Error())
			gi.stats.incrTotalSysExceptions(err)
			return nil, err
		}
		gi.consumers[topicName.Name] = consumer
	}
	return channel, nil
}

func (gi *goInstance) handlerMsg(input pulsar.Message) (output []byte, err error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	gi.context.SetCurrentRecord(input)

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
				gi.stats.incrTotalSysExceptions(err)
				log.Fatal(err)
			} else {
				if autoAck && !atMostOnce {
					gi.ackInputMessage(msgInput)
				}
				gi.stats.incrTotalProcessedSuccessfully()
			}
		})
	} else if autoAck && atLeastOnce {
		gi.ackInputMessage(msgInput)
		// Report that we processed successfully even though it's not going to an output topic?
		// We probably shouldn't...
		// gi.stats.incrTotalProcessedSuccessfully()
	}
}

// ackInputMessage doesn't produce any result, or the user doesn't want the result.
func (gi *goInstance) ackInputMessage(inputMessage pulsar.Message) {
	log.Debugf("ack input message topic name is: %s", inputMessage.Topic())
	gi.respondMessage(inputMessage, true)
}

func (gi *goInstance) nackInputMessage(inputMessage pulsar.Message) {
	gi.respondMessage(inputMessage, false)
}

func (gi *goInstance) respondMessage(inputMessage pulsar.Message, ack bool) {
	topicName, err := ParseTopicName(inputMessage.Topic())
	if err != nil {
		log.Errorf("unable respond to message ID %s - invalid topic: %v", messageIDStr(inputMessage), err)
		return
	}
	// consumers are indexed by topic name only (no partition)
	if ack {
		gi.consumers[topicName.NameWithoutPartition()].Ack(inputMessage)
		return
	}
	gi.consumers[topicName.NameWithoutPartition()].Nack(inputMessage)
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
	status := pb.FunctionStatus{}
	status.Running = true
	totalReceived := gi.getTotalReceived()
	totalProcessedSuccessfully := gi.getTotalProcessedSuccessfully()
	totalUserExceptions := gi.getTotalUserExceptions()
	totalSysExceptions := gi.getTotalSysExceptions()
	avgProcessLatencyMs := gi.getAvgProcessLatency()
	lastInvocation := gi.getLastInvocation()

	status.NumReceived = int64(totalReceived)
	status.NumSuccessfullyProcessed = int64(totalProcessedSuccessfully)
	status.NumUserExceptions = int64(totalUserExceptions)
	status.InstanceId = strconv.Itoa(gi.context.instanceConf.instanceID)

	status.NumUserExceptions = int64(totalUserExceptions)
	for _, exPair := range gi.stats.latestUserException {
		toAdd := pb.FunctionStatus_ExceptionInformation{}
		toAdd.ExceptionString = exPair.exception.Error()
		toAdd.MsSinceEpoch = exPair.timestamp
		status.LatestUserExceptions = append(status.LatestUserExceptions, &toAdd)
	}

	status.NumSystemExceptions = int64(totalSysExceptions)
	for _, exPair := range gi.stats.latestSysException {
		toAdd := pb.FunctionStatus_ExceptionInformation{}
		toAdd.ExceptionString = exPair.exception.Error()
		toAdd.MsSinceEpoch = exPair.timestamp
		status.LatestSystemExceptions = append(status.LatestSystemExceptions, &toAdd)
	}
	status.AverageLatency = float64(avgProcessLatencyMs)
	status.LastInvocationTime = int64(lastInvocation)
	return &status
}

func (gi *goInstance) getMetrics() *pb.MetricsData {
	totalReceived := gi.getTotalReceived()
	totalProcessedSuccessfully := gi.getTotalProcessedSuccessfully()
	totalUserExceptions := gi.getTotalUserExceptions()
	totalSysExceptions := gi.getTotalSysExceptions()
	avgProcessLatencyMs := gi.getAvgProcessLatency()
	lastInvocation := gi.getLastInvocation()

	totalReceived1min := gi.getTotalReceived1min()
	totalProcessedSuccessfully1min := gi.getTotalProcessedSuccessfully1min()
	totalUserExceptions1min := gi.getTotalUserExceptions1min()
	totalSysExceptions1min := gi.getTotalSysExceptions1min()
	//avg_process_latency_ms_1min := gi.get_avg_process_latency_1min()

	metricsData := pb.MetricsData{}
	// total metrics
	metricsData.ReceivedTotal = int64(totalReceived)
	metricsData.ProcessedSuccessfullyTotal = int64(totalProcessedSuccessfully)
	metricsData.SystemExceptionsTotal = int64(totalSysExceptions)
	metricsData.UserExceptionsTotal = int64(totalUserExceptions)
	metricsData.AvgProcessLatency = float64(avgProcessLatencyMs)
	metricsData.LastInvocation = int64(lastInvocation)
	// 1min metrics
	metricsData.ReceivedTotal_1Min = int64(totalReceived1min)
	metricsData.ProcessedSuccessfullyTotal_1Min = int64(totalProcessedSuccessfully1min)
	metricsData.SystemExceptionsTotal_1Min = int64(totalSysExceptions1min)
	metricsData.UserExceptionsTotal_1Min = int64(totalUserExceptions1min)

	return &metricsData
}

func (gi *goInstance) getAndResetMetrics() *pb.MetricsData {
	metricsData := gi.getMetrics()
	gi.resetMetrics()
	return metricsData
}

func (gi *goInstance) resetMetrics() *empty.Empty {
	gi.stats.reset()
	return &empty.Empty{}
}

// This method is used to get the required metrics for Prometheus.
// Note that this doesn't distinguish between parallel function instances!
func (gi *goInstance) getMatchingMetricFunc() func(lbl *prometheus_client.LabelPair) bool {
	matchMetricFunc := func(lbl *prometheus_client.LabelPair) bool {
		return *lbl.Name == "fqfn" && *lbl.Value == gi.context.GetTenantAndNamespaceAndName()
	}
	return matchMetricFunc
}

func (gi *goInstance) getMatchingMetricFromRegistry(metricName string) prometheus_client.Metric {
	metricFamilies, err := reg.Gather()
	if err != nil {
		log.Errorf("Something went wrong when calling reg.Gather(), the metricName is: %s", metricName)
	}
	matchFamilyFunc := func(vect *prometheus_client.MetricFamily) bool {
		return *vect.Name == metricName
	}
	filteredMetricFamilies := filter(metricFamilies, matchFamilyFunc)
	if len(filteredMetricFamilies) > 1 {
		// handle this.
		log.Errorf("Too many metric families for metricName: %s " + metricName)
	}
	metricFunc := gi.getMatchingMetricFunc()
	matchingMetric := getFirstMatch(filteredMetricFamilies[0].Metric, metricFunc)
	return *matchingMetric
}

func (gi *goInstance) getTotalReceived() float32 {
	// "pulsar_function_" + "received_total", NewGaugeVec.
	metric := gi.getMatchingMetricFromRegistry(PulsarFunctionMetricsPrefix + TotalReceived)
	val := metric.GetGauge().Value
	return float32(*val)
}

func (gi *goInstance) getTotalProcessedSuccessfully() float32 {
	metric := gi.getMatchingMetricFromRegistry(PulsarFunctionMetricsPrefix + TotalSuccessfullyProcessed)
	// "pulsar_function_" + "processed_successfully_total", NewGaugeVec.
	val := metric.GetGauge().Value
	return float32(*val)
}

func (gi *goInstance) getTotalSysExceptions() float32 {
	metric := gi.getMatchingMetricFromRegistry(PulsarFunctionMetricsPrefix + TotalSystemExceptions)
	// "pulsar_function_"+ "system_exceptions_total", NewGaugeVec.
	val := metric.GetGauge().Value
	return float32(*val)
}

func (gi *goInstance) getTotalUserExceptions() float32 {
	metric := gi.getMatchingMetricFromRegistry(PulsarFunctionMetricsPrefix + TotalUserExceptions)
	// "pulsar_function_" + "user_exceptions_total", NewGaugeVec
	val := metric.GetGauge().Value
	return float32(*val)
}

func (gi *goInstance) getAvgProcessLatency() float32 {
	metric := gi.getMatchingMetricFromRegistry(PulsarFunctionMetricsPrefix + ProcessLatencyMs)
	// "pulsar_function_" + "process_latency_ms", SummaryVec.
	count := metric.GetSummary().SampleCount
	sum := metric.GetSummary().SampleSum
	if *count <= 0.0 {
		return 0.0
	}
	return float32(*sum) / float32(*count)
}

func (gi *goInstance) getLastInvocation() float32 {
	metric := gi.getMatchingMetricFromRegistry(PulsarFunctionMetricsPrefix + LastInvocation)
	// "pulsar_function_" + "last_invocation", GaugeVec.
	val := metric.GetGauge().Value
	return float32(*val)
}

func (gi *goInstance) getTotalProcessedSuccessfully1min() float32 {
	metric := gi.getMatchingMetricFromRegistry(PulsarFunctionMetricsPrefix + TotalSuccessfullyProcessed1min)
	// "pulsar_function_" + "processed_successfully_total_1min", GaugeVec.
	val := metric.GetGauge().Value
	return float32(*val)
}

func (gi *goInstance) getTotalSysExceptions1min() float32 {
	metric := gi.getMatchingMetricFromRegistry(PulsarFunctionMetricsPrefix + TotalSystemExceptions1min)
	// "pulsar_function_" + "system_exceptions_total_1min", GaugeVec
	val := metric.GetGauge().Value
	return float32(*val)
}

func (gi *goInstance) getTotalUserExceptions1min() float32 {
	metric := gi.getMatchingMetricFromRegistry(PulsarFunctionMetricsPrefix + TotalUserExceptions1min)
	// "pulsar_function_" + "user_exceptions_total_1min", GaugeVec
	val := metric.GetGauge().Value
	return float32(*val)
}

func (gi *goInstance) getTotalReceived1min() float32 {
	metric := gi.getMatchingMetricFromRegistry(PulsarFunctionMetricsPrefix + TotalReceived1min)
	// "pulsar_function_" +  "received_total_1min", GaugeVec
	val := metric.GetGauge().Value
	return float32(*val)
}
