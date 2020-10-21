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

func (m *MockMessage) RedeliveryCount() uint32 {
	return 1
}

func (m *MockMessage) IsReplicated() bool {
	return true
}

func (m *MockMessage) GetReplicatedFrom() string {
	return "mock-cluster"
}

type MockMessageID struct{}

func (m *MockMessageID) Serialize() []byte {
	return []byte(`message-id`)
}
