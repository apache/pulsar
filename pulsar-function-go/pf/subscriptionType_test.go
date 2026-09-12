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

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/stretchr/testify/assert"

	pb "github.com/apache/pulsar/pulsar-function-go/pb"
)

// The Go runtime previously read only SubscriptionType, so retainOrdering and retainKeyOrdering
// were accepted and silently dropped: a function created with --retain-key-ordering ran on a
// Shared subscription and lost per-key ordering. These pin the rules the Java and Python runtimes
// apply.
func TestResolveSubscriptionType(t *testing.T) {
	tests := []struct {
		name              string
		configured        pb.SubscriptionType
		retainOrdering    bool
		retainKeyOrdering bool
		expected          pulsar.SubscriptionType
	}{
		{
			name:       "default is shared",
			configured: pb.SubscriptionType_SHARED,
			expected:   pulsar.Shared,
		},
		{
			name:       "explicit failover is honoured",
			configured: pb.SubscriptionType_FAILOVER,
			expected:   pulsar.Failover,
		},
		{
			name:           "retainOrdering selects failover",
			configured:     pb.SubscriptionType_SHARED,
			retainOrdering: true,
			expected:       pulsar.Failover,
		},
		{
			name:              "retainKeyOrdering selects key_shared",
			configured:        pb.SubscriptionType_SHARED,
			retainKeyOrdering: true,
			expected:          pulsar.KeyShared,
		},
		{
			// python_instance.py applies retainOrdering first and only falls to retainKeyOrdering
			// in the else branch, so ordering wins. Pinned so the two runtimes cannot drift.
			name:              "retainOrdering wins over retainKeyOrdering",
			configured:        pb.SubscriptionType_SHARED,
			retainOrdering:    true,
			retainKeyOrdering: true,
			expected:          pulsar.Failover,
		},
		{
			name:              "retainKeyOrdering overrides an explicit failover",
			configured:        pb.SubscriptionType_FAILOVER,
			retainKeyOrdering: true,
			expected:          pulsar.KeyShared,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expected,
				resolveSubscriptionType(test.configured, test.retainOrdering, test.retainKeyOrdering))
		})
	}
}
