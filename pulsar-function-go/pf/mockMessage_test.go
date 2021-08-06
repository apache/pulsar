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
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
)

type MockMessage struct {
	properties map[string]string
	messageID  *MockMessageID
	payload    []byte
}

func (m *MockMessage) Topic() string {
	return ""
}

func (m *MockMessage) ProducerName() string {
	return "mock-producer"
}

func (m *MockMessage) Properties() map[string]string {
	return m.properties
}

func (m *MockMessage) Payload() []byte {
	return m.payload
}

func (m *MockMessage) ID() pulsar.MessageID {
	return m.messageID
}

func (m *MockMessage) PublishTime() time.Time {
	return time.Now()
}

func (m *MockMessage) EventTime() time.Time {
	return time.Now()
}

func (m *MockMessage) Key() string {
	return "key"
}

func (m *MockMessage) OrderingKey() string {
	return "orderingKey"
}

func (m *MockMessage) RedeliveryCount() uint32 {
	return 1
}

func (m *MockMessage) IsReplicated() bool {
	return true
}

func (m *MockMessage) GetReplicatedFrom() string {
	return "mock-cluster"
}

func (m *MockMessage) GetSchemaValue(v interface{}) error {
	return nil
}

type MockMessageID struct{}

func (m *MockMessageID) Serialize() []byte {
	return []byte(`message-id`)
}

func (m *MockMessageID) LedgerID() int64 {
	return 0
}

func (m *MockMessageID) EntryID() int64 {
	return 0
}

func (m *MockMessageID) BatchIdx() int32 {
	return 0
}

func (m *MockMessageID) PartitionIdx() int32 {
	return 0
}

type MockPulsarProducer struct{}

func (producer *MockPulsarProducer) Topic() string {
	return "publish-topic"
}

func (producer *MockPulsarProducer) Name() string {
	return "publish-producer"
}

func (producer *MockPulsarProducer) Send(context.Context, *pulsar.ProducerMessage) (pulsar.MessageID, error) {
	return nil, nil
}

func (producer *MockPulsarProducer) SendAsync(context.Context, *pulsar.ProducerMessage,
	func(pulsar.MessageID, *pulsar.ProducerMessage, error)) {
}

func (producer *MockPulsarProducer) LastSequenceID() int64 {
	return int64(10)
}

func (producer *MockPulsarProducer) Flush() error {
	return nil
}

func (producer *MockPulsarProducer) Close() {
}
