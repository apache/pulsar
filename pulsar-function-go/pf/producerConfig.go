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
	"time"

	"github.com/apache/pulsar-client-go/pulsar"

	pb "github.com/apache/pulsar/pulsar-function-go/pb"
)

const (
	// defaultCompressionType and defaultBatchingMaxPublishDelay are the settings a function gets when
	// it carries no producer configuration. They mirror the base defaults of the Java runtime's
	// ProducerBuilderFactory (enableBatching(true), batchingMaxPublishDelay(10ms)) so that a function
	// behaves the same on either runtime when nothing is configured.
	defaultCompressionType         = pulsar.LZ4
	defaultBatchingMaxPublishDelay = 10 * time.Millisecond
	keyBasedBatchBuilder           = "KEY_BASED"
)

// batcherBuilderType translates a batchBuilder name from a function's ProducerSpec into a
// pulsar.BatcherBuilderType. Anything other than "KEY_BASED" maps to the default batcher, matching
// the Java runtime.
func batcherBuilderType(batchBuilder string) pulsar.BatcherBuilderType {
	if batchBuilder == keyBasedBatchBuilder {
		return pulsar.KeyBasedBatchBuilder
	}
	return pulsar.DefaultBatchBuilder
}

// producerOptionsFromSpec builds the producer options a function's sink ProducerSpec configures.
//
// It returns only the settings the spec owns; the caller fills in the rest (topic, properties, send
// timeout, ...). The mapping is:
//
//	ProducerSpec field                        pulsar.ProducerOptions field
//	--------------------------------------    ----------------------------------
//	CompressionType                           CompressionType
//	BatchBuilder                              BatcherBuilderType
//	MaxPendingMessages                        MaxPendingMessages
//	BatchingSpec.Enabled                      DisableBatching (inverted)
//	BatchingSpec.BatchingMaxPublishDelayMs    BatchingMaxPublishDelay
//	BatchingSpec.BatchingMaxMessages          BatchingMaxMessages
//	BatchingSpec.BatchingMaxBytes             BatchingMaxSize
//	BatchingSpec.BatchBuilder                 BatcherBuilderType (wins over BatchBuilder above)
//
// Three rules keep this aligned with the Java runtime (ProducerBuilderFactory and BatchingUtils):
//
//   - A field that is unset or non-positive in the spec is left at its zero value, so the client's
//     own default applies rather than an explicit zero being pushed onto the producer.
//   - A nil spec, or a spec with no BatchingSpec, yields the backwards-compatible defaults: batching
//     enabled with a 10ms maximum publish delay. This mirrors BatchingUtils.convertFromSpec(nil) and
//     is what functions written before batching became configurable already run with.
//   - BatchingSpec.BatchBuilder wins over ProducerSpec.BatchBuilder, because the Java runtime applies
//     them in that order.
//
// BatchingSpec.RoundRobinRouterBatchingPartitionSwitchFrequency and
// ProducerSpec.MaxPendingMessagesAcrossPartitions have no equivalent in the Go client and are
// ignored. DisableBlockIfQueueFull is left false (the producer blocks), matching the Java runtime,
// which hardcodes blockIfQueueFull(true) and exposes no configuration for it.
func producerOptionsFromSpec(spec *pb.ProducerSpec) pulsar.ProducerOptions {
	options := pulsar.ProducerOptions{
		CompressionType:         defaultCompressionType,
		BatchingMaxPublishDelay: defaultBatchingMaxPublishDelay,
		BatcherBuilderType:      pulsar.DefaultBatchBuilder,
	}

	if spec == nil {
		return options
	}

	switch spec.CompressionType {
	case pb.CompressionType_NONE:
		options.CompressionType = pulsar.NoCompression
	case pb.CompressionType_ZLIB:
		options.CompressionType = pulsar.ZLib
	case pb.CompressionType_ZSTD:
		options.CompressionType = pulsar.ZSTD
	default:
		// the Go client does not support SNAPPY yet, so LZ4 covers both LZ4 and SNAPPY
		options.CompressionType = pulsar.LZ4
	}

	// batchBuilder lives on the ProducerSpec itself and, since PIP-401, also on the nested
	// BatchingSpec. The Java runtime applies the ProducerSpec one first and lets the BatchingSpec one
	// override it, so do the same here.
	if spec.BatchBuilder != "" {
		options.BatcherBuilderType = batcherBuilderType(spec.BatchBuilder)
	}

	if spec.MaxPendingMessages > 0 {
		options.MaxPendingMessages = int(spec.MaxPendingMessages)
	}

	batchingSpec := spec.GetBatchingSpec()
	if batchingSpec == nil {
		return options
	}

	options.DisableBatching = !batchingSpec.Enabled
	if batchingSpec.BatchingMaxPublishDelayMs > 0 {
		options.BatchingMaxPublishDelay = time.Duration(batchingSpec.BatchingMaxPublishDelayMs) * time.Millisecond
	}
	if batchingSpec.BatchingMaxMessages > 0 {
		options.BatchingMaxMessages = uint(batchingSpec.BatchingMaxMessages)
	}
	if batchingSpec.BatchingMaxBytes > 0 {
		options.BatchingMaxSize = uint(batchingSpec.BatchingMaxBytes)
	}
	if batchingSpec.BatchBuilder != "" {
		options.BatcherBuilderType = batcherBuilderType(batchingSpec.BatchBuilder)
	}

	return options
}
