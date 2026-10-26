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
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/stretchr/testify/assert"

	pb "github.com/apache/pulsar/pulsar-function-go/pb"
)

// A function with no producer configuration must keep the behaviour it had before batching became
// configurable: batching on, 10ms maximum publish delay, LZ4, default batcher.
func TestProducerOptionsFromSpec_NilSpecUsesDefaults(t *testing.T) {
	options := producerOptionsFromSpec(nil)

	assert.False(t, options.DisableBatching)
	assert.Equal(t, 10*time.Millisecond, options.BatchingMaxPublishDelay)
	assert.Equal(t, pulsar.LZ4, options.CompressionType)
	assert.Equal(t, pulsar.DefaultBatchBuilder, options.BatcherBuilderType)
	assert.Zero(t, options.BatchingMaxMessages)
	assert.Zero(t, options.BatchingMaxSize)
	assert.Zero(t, options.MaxPendingMessages)
}

func TestProducerOptionsFromSpec_NoBatchingSpecUsesDefaults(t *testing.T) {
	options := producerOptionsFromSpec(&pb.ProducerSpec{
		CompressionType: pb.CompressionType_ZSTD,
	})

	assert.False(t, options.DisableBatching)
	assert.Equal(t, 10*time.Millisecond, options.BatchingMaxPublishDelay)
	assert.Equal(t, pulsar.ZSTD, options.CompressionType)
	assert.Zero(t, options.BatchingMaxMessages)
}

func TestProducerOptionsFromSpec_BatchingCanBeDisabled(t *testing.T) {
	options := producerOptionsFromSpec(&pb.ProducerSpec{
		BatchingSpec: &pb.BatchingSpec{Enabled: false},
	})

	assert.True(t, options.DisableBatching)
	// the default delay is still set; it is inert while batching is off
	assert.Equal(t, 10*time.Millisecond, options.BatchingMaxPublishDelay)
}

func TestProducerOptionsFromSpec_FullBatchingSpecIsTranslated(t *testing.T) {
	options := producerOptionsFromSpec(&pb.ProducerSpec{
		BatchingSpec: &pb.BatchingSpec{
			Enabled:                   true,
			BatchingMaxPublishDelayMs: 1,
			BatchingMaxMessages:       500,
			BatchingMaxBytes:          65536,
			BatchBuilder:              "KEY_BASED",
		},
	})

	assert.False(t, options.DisableBatching)
	assert.Equal(t, 1*time.Millisecond, options.BatchingMaxPublishDelay)
	assert.Equal(t, uint(500), options.BatchingMaxMessages)
	assert.Equal(t, uint(65536), options.BatchingMaxSize)
	assert.Equal(t, pulsar.KeyBasedBatchBuilder, options.BatcherBuilderType)
}

// An explicit zero means "unset" in the protobuf, so the client default must apply rather than a
// literal zero being pushed onto the producer.
func TestProducerOptionsFromSpec_NonPositiveValuesFallBackToClientDefaults(t *testing.T) {
	options := producerOptionsFromSpec(&pb.ProducerSpec{
		MaxPendingMessages: 0,
		BatchingSpec: &pb.BatchingSpec{
			Enabled:                   true,
			BatchingMaxPublishDelayMs: 0,
			BatchingMaxMessages:       0,
			BatchingMaxBytes:          0,
		},
	})

	assert.Equal(t, 10*time.Millisecond, options.BatchingMaxPublishDelay)
	assert.Zero(t, options.BatchingMaxMessages)
	assert.Zero(t, options.BatchingMaxSize)
	assert.Zero(t, options.MaxPendingMessages)
}

func TestProducerOptionsFromSpec_MaxPendingMessagesIsTranslated(t *testing.T) {
	options := producerOptionsFromSpec(&pb.ProducerSpec{MaxPendingMessages: 2000})

	assert.Equal(t, 2000, options.MaxPendingMessages)
}

func TestProducerOptionsFromSpec_ProducerSpecBatchBuilderIsHonoured(t *testing.T) {
	options := producerOptionsFromSpec(&pb.ProducerSpec{BatchBuilder: "KEY_BASED"})

	assert.Equal(t, pulsar.KeyBasedBatchBuilder, options.BatcherBuilderType)
}

// The Java runtime applies BatchingSpec.batchBuilder after ProducerSpec.batchBuilder
// (ProducerBuilderFactory), so the nested value must win.
func TestProducerOptionsFromSpec_BatchingSpecBatchBuilderOverridesProducerSpec(t *testing.T) {
	options := producerOptionsFromSpec(&pb.ProducerSpec{
		BatchBuilder: "KEY_BASED",
		BatchingSpec: &pb.BatchingSpec{Enabled: true, BatchBuilder: "DEFAULT"},
	})

	assert.Equal(t, pulsar.DefaultBatchBuilder, options.BatcherBuilderType)
}

func TestProducerOptionsFromSpec_UnknownBatchBuilderFallsBackToDefault(t *testing.T) {
	options := producerOptionsFromSpec(&pb.ProducerSpec{BatchBuilder: "SOMETHING_ELSE"})

	assert.Equal(t, pulsar.DefaultBatchBuilder, options.BatcherBuilderType)
}

func TestProducerOptionsFromSpec_CompressionTypeIsTranslated(t *testing.T) {
	cases := []struct {
		name     string
		spec     pb.CompressionType
		expected pulsar.CompressionType
	}{
		{"LZ4", pb.CompressionType_LZ4, pulsar.LZ4},
		{"NONE", pb.CompressionType_NONE, pulsar.NoCompression},
		{"ZLIB", pb.CompressionType_ZLIB, pulsar.ZLib},
		{"ZSTD", pb.CompressionType_ZSTD, pulsar.ZSTD},
		// the Go client has no SNAPPY support, so it falls back to LZ4
		{"SNAPPY", pb.CompressionType_SNAPPY, pulsar.LZ4},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			options := producerOptionsFromSpec(&pb.ProducerSpec{CompressionType: tc.spec})
			assert.Equal(t, tc.expected, options.CompressionType)
		})
	}
}

// fakePulsarClient records the options getProducer builds so the wiring, and not just the
// translation, is covered.
type fakePulsarClient struct {
	pulsar.Client
	capturedOptions pulsar.ProducerOptions
}

func (c *fakePulsarClient) CreateProducer(options pulsar.ProducerOptions) (pulsar.Producer, error) {
	c.capturedOptions = options
	return nil, nil
}

func newProducerTestInstance(client pulsar.Client, producerSpec *pb.ProducerSpec) *goInstance {
	instance := &goInstance{
		client: client,
		context: &FunctionContext{
			instanceConf: &instanceConf{
				instanceID: 0,
				funcDetails: pb.FunctionDetails{
					Tenant:    "test-tenant",
					Namespace: "test-namespace",
					Name:      "test-function",
					Sink: &pb.SinkSpec{
						Topic:        "test-sink-topic",
						ProducerSpec: producerSpec,
					},
				},
			},
		},
		stats: NewStatWithLabelValues("", "", "", "", "", ""),
	}
	return instance
}

func TestGetProducer_DefaultsAreUnchangedWithoutAProducerSpec(t *testing.T) {
	client := &fakePulsarClient{}
	instance := newProducerTestInstance(client, nil)

	_, err := instance.getProducer("test-sink-topic")

	assert.Nil(t, err)
	assert.Equal(t, "test-sink-topic", client.capturedOptions.Topic)
	assert.False(t, client.capturedOptions.DisableBatching)
	assert.Equal(t, 10*time.Millisecond, client.capturedOptions.BatchingMaxPublishDelay)
	assert.Equal(t, pulsar.LZ4, client.capturedOptions.CompressionType)
	assert.Equal(t, time.Duration(0), client.capturedOptions.SendTimeout)
	assert.NotEmpty(t, client.capturedOptions.Properties)
}

func TestGetProducer_BatchingSpecReachesTheProducer(t *testing.T) {
	client := &fakePulsarClient{}
	instance := newProducerTestInstance(client, &pb.ProducerSpec{
		MaxPendingMessages: 500,
		BatchingSpec: &pb.BatchingSpec{
			Enabled:                   true,
			BatchingMaxPublishDelayMs: 2,
			BatchingMaxMessages:       100,
			BatchingMaxBytes:          4096,
		},
	})

	_, err := instance.getProducer("test-sink-topic")

	assert.Nil(t, err)
	assert.False(t, client.capturedOptions.DisableBatching)
	assert.Equal(t, 2*time.Millisecond, client.capturedOptions.BatchingMaxPublishDelay)
	assert.Equal(t, uint(100), client.capturedOptions.BatchingMaxMessages)
	assert.Equal(t, uint(4096), client.capturedOptions.BatchingMaxSize)
	assert.Equal(t, 500, client.capturedOptions.MaxPendingMessages)
}

func TestGetProducer_BatchingCanBeDisabled(t *testing.T) {
	client := &fakePulsarClient{}
	instance := newProducerTestInstance(client, &pb.ProducerSpec{
		BatchingSpec: &pb.BatchingSpec{Enabled: false},
	})

	_, err := instance.getProducer("test-sink-topic")

	assert.Nil(t, err)
	assert.True(t, client.capturedOptions.DisableBatching)
}

// getProducer serves both the sink producer and context.NewOutputMessage(), so a producer for
// another topic must be configured from the same spec.
func TestGetProducer_ContextOutputTopicUsesTheSameSpec(t *testing.T) {
	client := &fakePulsarClient{}
	instance := newProducerTestInstance(client, &pb.ProducerSpec{
		BatchBuilder: "KEY_BASED",
		BatchingSpec: &pb.BatchingSpec{Enabled: true, BatchingMaxPublishDelayMs: 3},
	})

	_, err := instance.getProducer("another-output-topic")

	assert.Nil(t, err)
	assert.Equal(t, "another-output-topic", client.capturedOptions.Topic)
	assert.Equal(t, 3*time.Millisecond, client.capturedOptions.BatchingMaxPublishDelay)
	assert.Equal(t, pulsar.KeyBasedBatchBuilder, client.capturedOptions.BatcherBuilderType)
}
