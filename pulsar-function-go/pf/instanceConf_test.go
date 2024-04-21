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
	"fmt"
	"testing"

	cfg "github.com/apache/pulsar/pulsar-function-go/conf"

	pb "github.com/apache/pulsar/pulsar-function-go/pb"
	"github.com/stretchr/testify/assert"
)

func Test_newInstanceConf(t *testing.T) {
	assert.Equal(
		t,
		&instanceConf{
			instanceID:                  101,
			funcID:                      "pulsar-function",
			funcVersion:                 "1.0.0",
			maxBufTuples:                10,
			port:                        8091,
			clusterName:                 "pulsar-function-go",
			pulsarServiceURL:            "pulsar://localhost:6650",
			killAfterIdle:               50000,
			expectedHealthCheckInterval: 3,
			metricsPort:                 50001,
			funcDetails: pb.FunctionDetails{Tenant: "",
				Namespace:            "",
				Name:                 "go-function",
				ClassName:            "",
				LogTopic:             "log-topic",
				ProcessingGuarantees: 0,
				UserConfig:           `{"word-of-the-day": "hapax legomenon"}`,
				SecretsMap:           "",
				Runtime:              0,
				AutoAck:              true,
				Parallelism:          0,
				Source: &pb.SourceSpec{
					SubscriptionType: pb.SubscriptionType(0),
					InputSpecs: map[string]*pb.ConsumerSpec{
						"persistent://public/default/topic-01": {
							SchemaType:     "",
							IsRegexPattern: false,
							ReceiverQueueSize: &pb.ConsumerSpec_ReceiverQueueSize{
								Value: 10,
							},
						},
					},
					TimeoutMs:            0,
					SubscriptionName:     "",
					CleanupSubscription:  false,
					SubscriptionPosition: pb.SubscriptionPosition_EARLIEST,
				},
				Sink: &pb.SinkSpec{
					Topic:      "persistent://public/default/topic-02",
					SchemaType: "",
				},
				Resources: &pb.Resources{
					Cpu:  0,
					Ram:  0,
					Disk: 0,
				},
				PackageUrl: "",
				RetryDetails: &pb.RetryDetails{
					MaxMessageRetries: 0,
					DeadLetterTopic:   "",
				},
				RuntimeFlags:         "",
				ComponentType:        0,
				CustomRuntimeOptions: "",
			},
		},
		newInstanceConf(),
	)
}

func TestInstanceConf_GetInstanceName(t *testing.T) {
	instanceConf := newInstanceConf()
	instanceName := instanceConf.getInstanceName()

	assert.Equal(t, "101", instanceName)
}

func TestInstanceConf_Fail(t *testing.T) {
	assert.Panics(t, func() {
		newInstanceConfWithConf(&cfg.Conf{ProcessingGuarantees: 0, AutoACK: false})
	}, "Should have a panic")
	assert.Panics(t, func() {
		newInstanceConfWithConf(&cfg.Conf{ProcessingGuarantees: 1, AutoACK: false})
	}, "Should have a panic")
	assert.Panics(t, func() {
		newInstanceConfWithConf(&cfg.Conf{ProcessingGuarantees: 2})
	}, "Should have a panic")
	assert.NotPanicsf(t, func() {
		newInstanceConfWithConf(&cfg.Conf{ProcessingGuarantees: 3})
	}, "Should have a panic")
}

func TestInstanceConf_WithDetails(t *testing.T) {
	cfg := &cfg.Conf{
		FunctionDetails: `{"tenant":"public","namespace":"default","name":"test-function","className":"process",
"logTopic":"test-logs","userConfig":"{\"key1\":\"value1\"}","runtime":"GO","autoAck":true,"parallelism":1,
"source":{"configs":"{\"username\":\"admin\"}","typeClassName":"string","timeoutMs":"15000",
"subscriptionName":"test-subscription","inputSpecs":{"input":{"schemaType":"avro","receiverQueueSize":{"value":1000},
"schemaProperties":{"schema_prop1":"schema1"},"consumerProperties":{"consumer_prop1":"consumer1"},"cryptoSpec":
{"cryptoKeyReaderClassName":"key-reader","producerCryptoFailureAction":"SEND","consumerCryptoFailureAction":"CONSUME"}}}
,"negativeAckRedeliveryDelayMs":"15000"},"sink":{"configs":"{\"password\":\"admin\"}","topic":"test-output",
"typeClassName":"string","schemaType":"avro","producerSpec":{"maxPendingMessages":2000,"useThreadLocalProducers":true,
"cryptoSpec":{"cryptoKeyReaderClassName":"key-reader","producerCryptoFailureAction":"DISCARD"},
"batchBuilder":"DEFAULT"}},"resources":{"cpu":2.0,"ram":"1024","disk":"1024"},"packageUrl":"/path/to/package",
"retryDetails":{"maxMessageRetries":3,"deadLetterTopic":"test-dead-letter-topic"},"secretsMap":
"{\"secret1\":\"secret-value1\"}","runtimeFlags":"flags","componentType":"FUNCTION","customRuntimeOptions":"options",
"retainOrdering":true,"retainKeyOrdering":true,"subscriptionPosition":"EARLIEST"}`,
	}
	instanceConf := newInstanceConfWithConf(cfg)
	assert.Equal(t, "public", instanceConf.funcDetails.Tenant)
	assert.Equal(t, "default", instanceConf.funcDetails.Namespace)
	assert.Equal(t, "test-function", instanceConf.funcDetails.Name)
	assert.Equal(t, "process", instanceConf.funcDetails.ClassName)
	assert.Equal(t, "test-logs", instanceConf.funcDetails.LogTopic)
	assert.Equal(t, pb.ProcessingGuarantees_ATLEAST_ONCE, instanceConf.funcDetails.ProcessingGuarantees)
	assert.Equal(t, `{"key1":"value1"}`, instanceConf.funcDetails.UserConfig)
	assert.Equal(t, `{"secret1":"secret-value1"}`, instanceConf.funcDetails.SecretsMap)
	assert.Equal(t, pb.FunctionDetails_GO, instanceConf.funcDetails.Runtime)

	assert.Equal(t, true, instanceConf.funcDetails.AutoAck)
	assert.Equal(t, int32(1), instanceConf.funcDetails.Parallelism)

	sourceSpec := pb.SourceSpec{
		TypeClassName:                "string",
		TimeoutMs:                    15000,
		Configs:                      `{"username":"admin"}`,
		SubscriptionName:             "test-subscription",
		SubscriptionType:             pb.SubscriptionType_SHARED,
		NegativeAckRedeliveryDelayMs: 15000,
		InputSpecs: map[string]*pb.ConsumerSpec{
			"input": {
				SchemaType: "avro",
				SchemaProperties: map[string]string{
					"schema_prop1": "schema1",
				},
				ConsumerProperties: map[string]string{
					"consumer_prop1": "consumer1",
				},
				ReceiverQueueSize: &pb.ConsumerSpec_ReceiverQueueSize{
					Value: 1000,
				},
				CryptoSpec: &pb.CryptoSpec{
					CryptoKeyReaderClassName:    "key-reader",
					ProducerCryptoFailureAction: pb.CryptoSpec_SEND,
					ConsumerCryptoFailureAction: pb.CryptoSpec_CONSUME,
				},
			},
		},
	}
	assert.Equal(t, sourceSpec.String(), instanceConf.funcDetails.Source.String())

	sinkSpec := pb.SinkSpec{
		TypeClassName: "string",
		Topic:         "test-output",
		Configs:       `{"password":"admin"}`,
		SchemaType:    "avro",
		ProducerSpec: &pb.ProducerSpec{
			MaxPendingMessages:      2000,
			UseThreadLocalProducers: true,
			CryptoSpec: &pb.CryptoSpec{
				CryptoKeyReaderClassName:    "key-reader",
				ProducerCryptoFailureAction: pb.CryptoSpec_DISCARD,
				ConsumerCryptoFailureAction: pb.CryptoSpec_FAIL,
			},
			BatchBuilder: "DEFAULT",
		},
	}
	assert.Equal(t, sinkSpec.String(), instanceConf.funcDetails.Sink.String())

	resource := pb.Resources{
		Cpu:  2.0,
		Ram:  1024,
		Disk: 1024,
	}
	assert.Equal(t, resource.String(), instanceConf.funcDetails.Resources.String())
	assert.Equal(t, "/path/to/package", instanceConf.funcDetails.PackageUrl)

	retryDetails := pb.RetryDetails{
		MaxMessageRetries: 3,
		DeadLetterTopic:   "test-dead-letter-topic",
	}
	assert.Equal(t, retryDetails.String(), instanceConf.funcDetails.RetryDetails.String())

	assert.Equal(t, "flags", instanceConf.funcDetails.RuntimeFlags)
	assert.Equal(t, pb.FunctionDetails_FUNCTION, instanceConf.funcDetails.ComponentType)
	assert.Equal(t, "options", instanceConf.funcDetails.CustomRuntimeOptions)
	assert.Equal(t, "", instanceConf.funcDetails.Builtin)
	assert.Equal(t, true, instanceConf.funcDetails.RetainOrdering)
	assert.Equal(t, true, instanceConf.funcDetails.RetainKeyOrdering)
	assert.Equal(t, pb.SubscriptionPosition_EARLIEST, instanceConf.funcDetails.SubscriptionPosition)
}

func TestInstanceConf_WithEmptyOrInvalidDetails(t *testing.T) {
	testCases := []struct {
		name    string
		details string
	}{
		{
			name:    "empty details",
			details: "",
		},
		{
			name:    "invalid details",
			details: "error",
		},
	}

	for i, testCase := range testCases {

		t.Run(fmt.Sprintf("testCase[%d] %s", i, testCase.name), func(t *testing.T) {
			cfg := &cfg.Conf{
				FunctionDetails:      testCase.details,
				Tenant:               "public",
				NameSpace:            "default",
				Name:                 "test-function",
				LogTopic:             "test-logs",
				ProcessingGuarantees: 0,
				UserConfig:           `{"key1":"value1"}`,
				SecretsMap:           `{"secret1":"secret-value1"}`,
				Runtime:              3,
				AutoACK:              true,
				Parallelism:          1,
				SubscriptionType:     1,
				TimeoutMs:            15000,
				SubscriptionName:     "test-subscription",
				CleanupSubscription:  false,
				SubscriptionPosition: 0,
				SinkSpecTopic:        "test-output",
				SinkSchemaType:       "avro",
				Cpu:                  2.0,
				Ram:                  1024,
				Disk:                 1024,
				MaxMessageRetries:    3,
				DeadLetterTopic:      "test-dead-letter-topic",
				SourceInputSpecs: map[string]string{
					"input": `{"schemaType":"avro","receiverQueueSize":{"value":1000},"schemaProperties":
{"schema_prop1":"schema1"},"consumerProperties":{"consumer_prop1":"consumer1"}}`,
				},
			}
			instanceConf := newInstanceConfWithConf(cfg)

			assert.Equal(t, "public", instanceConf.funcDetails.Tenant)
			assert.Equal(t, "default", instanceConf.funcDetails.Namespace)
			assert.Equal(t, "test-function", instanceConf.funcDetails.Name)
			assert.Equal(t, "test-logs", instanceConf.funcDetails.LogTopic)
			assert.Equal(t, pb.ProcessingGuarantees_ATLEAST_ONCE, instanceConf.funcDetails.ProcessingGuarantees)
			assert.Equal(t, `{"key1":"value1"}`, instanceConf.funcDetails.UserConfig)
			assert.Equal(t, `{"secret1":"secret-value1"}`, instanceConf.funcDetails.SecretsMap)
			assert.Equal(t, pb.FunctionDetails_GO, instanceConf.funcDetails.Runtime)

			assert.Equal(t, true, instanceConf.funcDetails.AutoAck)
			assert.Equal(t, int32(1), instanceConf.funcDetails.Parallelism)

			sourceSpec := pb.SourceSpec{
				SubscriptionType:     pb.SubscriptionType_FAILOVER,
				TimeoutMs:            15000,
				SubscriptionName:     "test-subscription",
				CleanupSubscription:  false,
				SubscriptionPosition: pb.SubscriptionPosition_LATEST,
				InputSpecs: map[string]*pb.ConsumerSpec{
					"input": {
						SchemaType: "avro",
						SchemaProperties: map[string]string{
							"schema_prop1": "schema1",
						},
						ConsumerProperties: map[string]string{
							"consumer_prop1": "consumer1",
						},
						ReceiverQueueSize: &pb.ConsumerSpec_ReceiverQueueSize{
							Value: 1000,
						},
					},
				},
			}
			assert.Equal(t, sourceSpec.String(), instanceConf.funcDetails.Source.String())

			sinkSpec := pb.SinkSpec{
				Topic:      "test-output",
				SchemaType: "avro",
			}
			assert.Equal(t, sinkSpec.String(), instanceConf.funcDetails.Sink.String())

			resource := pb.Resources{
				Cpu:  2.0,
				Ram:  1024,
				Disk: 1024,
			}
			assert.Equal(t, resource.String(), instanceConf.funcDetails.Resources.String())

			retryDetails := pb.RetryDetails{
				MaxMessageRetries: 3,
				DeadLetterTopic:   "test-dead-letter-topic",
			}
			assert.Equal(t, retryDetails.String(), instanceConf.funcDetails.RetryDetails.String())
		})
	}
}
