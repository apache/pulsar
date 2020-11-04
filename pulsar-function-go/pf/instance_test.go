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
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func testProcessSpawnerHealthCheckTimer(
	tkr *time.Ticker, lastHealthCheckTs int64, expectedHealthCheckInterval int32, counter *int) {
	fmt.Println("Starting processSpawnerHealthCheckTimer")
	now := time.Now()
	maxIdleTime := int64(time.Duration(expectedHealthCheckInterval) * 3 * time.Second)
	fmt.Println("maxIdleTime is: " + strconv.FormatInt(maxIdleTime, 10))
	timeSinceLastCheck := now.UnixNano() - lastHealthCheckTs
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
	lastHealthCheckTs := now.UnixNano()

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
				testProcessSpawnerHealthCheckTimer(tkr, lastHealthCheckTs, expectedHealthCheckInterval, counter)
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
