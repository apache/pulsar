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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/stretchr/testify/assert"

	log "github.com/apache/pulsar/pulsar-function-go/logutil"
)

// recordingProducer records the payloads sent to the log topic.
type recordingProducer struct {
	MockPulsarProducer
	mutex    sync.Mutex
	payloads []string
	closed   bool
}

func (p *recordingProducer) SendAsync(_ context.Context, msg *pulsar.ProducerMessage,
	_ func(pulsar.MessageID, *pulsar.ProducerMessage, error)) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.payloads = append(p.payloads, string(msg.Payload))
}

func (p *recordingProducer) Close() {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.closed = true
}

func (p *recordingProducer) sent(text string) bool {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	for _, payload := range p.payloads {
		if strings.Contains(payload, text) {
			return true
		}
	}
	return false
}

func TestLogAppenderFlushesPeriodically(t *testing.T) {
	log.DrainEntries()
	producer := &recordingProducer{}
	appender := &LogAppender{producer: producer}
	appender.startFlushing(10 * time.Millisecond)
	defer appender.Stop()

	log.Info("logged while no message is processed")

	assert.Eventually(t, func() bool {
		return producer.sent("logged while no message is processed")
	}, 5*time.Second, 10*time.Millisecond)
}

func TestLogAppenderStopFlushesRemainingEntries(t *testing.T) {
	log.DrainEntries()
	producer := &recordingProducer{}
	appender := &LogAppender{producer: producer}
	appender.startFlushing(time.Hour)

	log.Info("logged right before stopping")
	appender.Stop()

	assert.True(t, producer.sent("logged right before stopping"))
	assert.True(t, producer.closed)
	assert.Nil(t, appender.producer)
}

func TestAddLogTopicHandlerWithoutLogTopic(t *testing.T) {
	log.Info("logged without a log topic")
	instance := &goInstance{context: NewFuncContext()}

	instance.addLogTopicHandler()

	for _, entry := range log.DrainEntries() {
		assert.NotContains(t, entry, "logged without a log topic")
	}
}
