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
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/stretchr/testify/assert"
)

func TestContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fc := NewFuncContext()
	fc.record = &MockMessage{
		properties: map[string]string{"FOO": "BAR"},
		messageID:  &MockMessageID{},
	}
	ctx = NewContext(ctx, fc)

	if resfc, ok := FromContext(ctx); ok {
		assert.Equal(t, 101, resfc.GetInstanceID())
		assert.Equal(t, []string{"persistent://public/default/topic-01"}, resfc.GetInputTopics())
		assert.Equal(t, "persistent://public/default/topic-02", resfc.GetOutputTopic())
		assert.Equal(t, "/", resfc.GetTenantAndNamespace())
		assert.Equal(t, "//go-function", resfc.GetTenantAndNamespaceAndName())
		assert.Equal(t, "", resfc.GetFuncTenant())
		assert.Equal(t, "go-function", resfc.GetFuncName())
		assert.Equal(t, "", resfc.GetFuncNamespace())
		assert.Equal(t, "pulsar-function", resfc.GetFuncID())
		assert.Equal(t, 8091, resfc.GetPort())
		assert.Equal(t, "pulsar-function-go", resfc.GetClusterName())
		assert.Equal(t, int32(3), resfc.GetExpectedHealthCheckInterval())
		assert.Equal(t, time.Duration(3), resfc.GetExpectedHealthCheckIntervalAsDuration())
		assert.Equal(t, int64(9000000000), resfc.GetMaxIdleTime())
		assert.Equal(t, "1.0.0", resfc.GetFuncVersion())
		assert.Equal(t, "hapax legomenon", resfc.GetUserConfValue("word-of-the-day"))
		assert.Equal(
			t,
			map[string]interface{}{"word-of-the-day": "hapax legomenon"},
			resfc.GetUserConfMap())
		assert.IsType(t, &MockMessage{}, fc.GetCurrentRecord())
	}
}

func TestFunctionContext_setCurrentRecord(t *testing.T) {
	fc := NewFuncContext()

	fc.SetCurrentRecord(&MockMessage{})

	assert.IsType(t, &MockMessage{}, fc.record)
}

func TestFunctionContext_NewOutputMessage(t *testing.T) {
	fc := NewFuncContext()
	publishTopic := "publish-topic"

	fc.outputMessage = func(topic string) pulsar.Producer {
		return &MockPulsarProducer{}
	}

	actualProducer := fc.NewOutputMessage(publishTopic)
	assert.IsType(t, &MockPulsarProducer{}, actualProducer)
}
