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
	"encoding/base64"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	pb "github.com/apache/pulsar/pulsar-function-go/pb"
)

func testProcessSpawnerHealthCheckTimer(
	tkr *time.Ticker, lastHealthCheckTS int64, expectedHealthCheckInterval int32, counter *int) {
	fmt.Println("Starting processSpawnerHealthCheckTimer")
	now := time.Now()
	maxIdleTime := int64(time.Duration(expectedHealthCheckInterval) * 3 * time.Second)
	fmt.Println("maxIdleTime is: " + strconv.FormatInt(maxIdleTime, 10))
	timeSinceLastCheck := now.UnixNano() - lastHealthCheckTS
	fmt.Println("timeSinceLastCheck is: " + strconv.FormatInt(timeSinceLastCheck, 10))
	if (timeSinceLastCheck) > (maxIdleTime) {
		fmt.Println("Haven't received health check from spawner in a while. Stopping instance...")
		// os.Exit(1)
		tkr.Stop()
	} else {
		fmt.Println("Continuing to check")
		*counter++
	}
}

func testStartScheduler(counter *int) {
	now := time.Now()
	lastHealthCheckTS := now.UnixNano()

	var expectedHealthCheckInterval int32 = 1
	if expectedHealthCheckInterval > 0 {
		fmt.Println("Starting Scheduler")
		go func() {
			fmt.Println("Started Scheduler")
			period := time.Second * time.Duration(expectedHealthCheckInterval)
			fmt.Println("period is: " + period.String())
			tkr := time.NewTicker(period)
			for range tkr.C {
				fmt.Println("Starting Timer")
				testProcessSpawnerHealthCheckTimer(tkr, lastHealthCheckTS, expectedHealthCheckInterval, counter)
			}
		}()
	}
}

func TestInstance_HeartbeatTimer(t *testing.T) {
	counter := 0
	testStartScheduler(&counter)
	time.Sleep(time.Second * 10)
	assert.Equal(t, 2, counter)
}

func TestTime_EqualsThreeSecondsFixed(t *testing.T) {
	var expectedHealthCheckInterval int32 = 3
	timeAmount := time.Millisecond * 1000 * time.Duration(expectedHealthCheckInterval)
	assert.Equal(t, time.Second*3, timeAmount)
}
func TestTime_EqualsThreeSecondsTimed(t *testing.T) {
	start := time.Now()
	startTime := start.UnixNano()

	time.Sleep(time.Second * 3)

	end := time.Now()
	endTime := end.UnixNano()

	diff := endTime - startTime

	assert.True(t, time.Duration(diff) > time.Second*3)
	assert.True(t, time.Duration(diff) < time.Millisecond*3100)
}

type MockHandler struct{}

func (m *MockHandler) process(ctx context.Context, input []byte) ([]byte, error) {
	return []byte(`output`), nil
}

func Test_goInstance_handlerMsg(t *testing.T) {
	handler := &MockHandler{}
	fc := NewFuncContext()
	instance := &goInstance{
		function: handler,
		context:  fc,
	}
	message := &MockMessage{payload: []byte(`{}`)}

	output, err := instance.handlerMsg(message)

	assert.Nil(t, err)
	assert.Equal(t, "output", string(output))
	assert.Equal(t, message, fc.record)
}

func newTestGoInstance(guarantee pb.ProcessingGuarantees) *goInstance {
	return &goInstance{
		context: &FunctionContext{
			instanceConf: &instanceConf{
				funcDetails: pb.FunctionDetails{
					ProcessingGuarantees: guarantee,
				},
			},
		},
	}
}

func TestShouldNackInputOnFailure(t *testing.T) {
	tests := []struct {
		name      string
		guarantee pb.ProcessingGuarantees
		want      bool
	}{
		{"atLeastOnce", pb.ProcessingGuarantees_ATLEAST_ONCE, true},
		{"manual", pb.ProcessingGuarantees_MANUAL, true},
		{"atMostOnce", pb.ProcessingGuarantees_ATMOST_ONCE, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			instance := newTestGoInstance(tt.guarantee)
			assert.Equal(t, tt.want, instance.shouldNackInputOnFailure())
		})
	}
}

func TestProcessResultForwardsSourceMessageProperties(t *testing.T) {
	tests := []struct {
		name                    string
		forwardSourceProperties bool
		inputProperties         map[string]string
		wantProperties          map[string]string
	}{
		{
			name:                    "forwards source properties when enabled",
			forwardSourceProperties: true,
			inputProperties: map[string]string{
				"custom-key":           "custom-value",
				"__pfn_input_topic__":  "spoofed-topic",
				"__pfn_input_msg_id__": "spoofed-message-id",
			},
			wantProperties: map[string]string{
				"custom-key":           "custom-value",
				"__pfn_input_topic__":  "input-topic",
				"__pfn_input_msg_id__": base64.StdEncoding.EncodeToString([]byte("message-id")),
			},
		},
		{
			name:                    "does not forward source properties when disabled",
			forwardSourceProperties: false,
			inputProperties: map[string]string{
				"custom-key": "custom-value",
			},
			wantProperties: map[string]string{
				"__pfn_input_topic__":  "input-topic",
				"__pfn_input_msg_id__": base64.StdEncoding.EncodeToString([]byte("message-id")),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			instance := newTestGoInstance(pb.ProcessingGuarantees_ATMOST_ONCE)
			instance.context.instanceConf.funcDetails.Sink = &pb.SinkSpec{
				Topic:                        "output-topic",
				ForwardSourceMessageProperty: tt.forwardSourceProperties,
			}
			producer := &MockPulsarProducer{}
			instance.producer = producer
			input := &MockMessage{
				properties: tt.inputProperties,
				messageID:  &MockMessageID{},
				payload:    []byte("input"),
				topic:      "input-topic",
			}

			instance.processResult(input, []byte("output"))

			if assert.NotNil(t, producer.sentMessage) {
				assert.Equal(t, tt.wantProperties, producer.sentMessage.Properties)
			}
			assert.Equal(t, tt.inputProperties, input.Properties())
		})
	}
}
