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

	"github.com/stretchr/testify/assert"
)

// The Go runtime nacks on failure but never configured the delay, so the client default of 60s
// applied regardless of SourceSpec.NegativeAckRedeliveryDelayMs.
func TestResolveNackRedeliveryDelay(t *testing.T) {
	tests := []struct {
		name     string
		delayMs  uint64
		expected time.Duration
	}{
		{
			// proto3 scalar with no presence: unset reads as 0. Returning zero leaves
			// ConsumerOptions at its zero value, which the client treats as unset.
			name:     "unset leaves the client default",
			delayMs:  0,
			expected: 0,
		},
		{
			name:     "milliseconds are converted to a duration",
			delayMs:  5000,
			expected: 5 * time.Second,
		},
		{
			name:     "sub-second values are preserved",
			delayMs:  250,
			expected: 250 * time.Millisecond,
		},
		{
			name:     "the client default expressed explicitly still round-trips",
			delayMs:  60000,
			expected: time.Minute,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expected, resolveNackRedeliveryDelay(test.delayMs))
		})
	}
}
