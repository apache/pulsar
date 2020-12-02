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

	"github.com/stretchr/testify/assert"
)

func TestParseTopicName(t *testing.T) {
	testCases := []struct {
		in        string
		name      string
		namespace string
		partition int
	}{
		{in: "persistent://my-tenant/my-ns/my-topic",
			name:      "persistent://my-tenant/my-ns/my-topic",
			namespace: "my-tenant/my-ns", partition: -1},
		{in: "my-topic", name: "persistent://public/default/my-topic",
			namespace: "public/default", partition: -1},
		{in: "my-tenant/my-namespace/my-topic",
			name:      "persistent://my-tenant/my-namespace/my-topic",
			namespace: "my-tenant/my-namespace", partition: -1},
		{in: "non-persistent://my-tenant/my-namespace/my-topic",
			name:      "non-persistent://my-tenant/my-namespace/my-topic",
			namespace: "my-tenant/my-namespace", partition: -1},
		{in: "my-topic-partition-5",
			name:      "persistent://public/default/my-topic-partition-5",
			namespace: "public/default", partition: 5},
		// V1 topic name
		{in: "persistent://my-tenant/my-cluster/my-ns/my-topic",
			name:      "persistent://my-tenant/my-cluster/my-ns/my-topic",
			namespace: "my-tenant/my-cluster/my-ns", partition: -1},
		{in: "my-tenant/my-cluster/my-ns/my-topic",
			name:      "persistent://my-tenant/my-cluster/my-ns/my-topic",
			namespace: "my-tenant/my-cluster/my-ns", partition: -1},
	}
	for _, testCase := range testCases {
		t.Run(testCase.in, func(t *testing.T) {
			topic, err := ParseTopicName(testCase.in)
			assert.Nil(t, err)
			assert.Equal(t, testCase.name, topic.Name)
			assert.Equal(t, testCase.namespace, topic.Namespace)
			assert.Equal(t, testCase.partition, topic.Partition)
		})
	}
}

func TestParseTopicNameErrors(t *testing.T) {
	testCases := []string{
		"invalid://my-tenant/my-ns/my-topic",
		"invalid://my-tenant/my-ns/my-topic-partition-xyz",
		"my-tenant/my-ns/my-topic-partition-xyz/invalid",
		"persistent://my-tenant",
		"persistent://my-tenant/my-namespace",
		"persistent://my-tenant/my-cluster/my-ns/my-topic-partition-xyz/invalid",
	}
	for _, testCase := range testCases {
		t.Run(testCase, func(t *testing.T) {
			_, err := ParseTopicName(testCase)
			assert.NotNil(t, err)
		})
	}
}
