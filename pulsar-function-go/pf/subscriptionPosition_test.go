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
	pb "github.com/apache/pulsar/pulsar-function-go/pb"
	"github.com/stretchr/testify/assert"
)

func TestResolveSubscriptionPosition(t *testing.T) {
	tests := []struct {
		name     string
		position pb.SubscriptionPosition
		expected pulsar.SubscriptionInitialPosition
	}{
		{name: "latest", position: pb.SubscriptionPosition_LATEST, expected: pulsar.SubscriptionPositionLatest},
		{name: "earliest", position: pb.SubscriptionPosition_EARLIEST, expected: pulsar.SubscriptionPositionEarliest},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expected, resolveSubscriptionPosition(test.position))
		})
	}
}
