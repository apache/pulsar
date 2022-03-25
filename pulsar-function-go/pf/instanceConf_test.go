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

	pb "github.com/apache/pulsar/pulsar-function-go/pb"
	"github.com/stretchr/testify/assert"
)

func Test_newInstanceConf(t *testing.T) {
	assert.Equal(
		t,
		&instanceConf{
			instanceID:                  101,
			funcID:                      "pulsar-function",
			funcVersion:                 "1.0.0",
			maxBufTuples:                10,
			port:                        8091,
			clusterName:                 "pulsar-function-go",
			pulsarServiceURL:            "pulsar://localhost:6650",
			killAfterIdle:               50000,
			expectedHealthCheckInterval: 3,
			metricsPort:                 50001,
			funcDetails: pb.FunctionDetails{Tenant: "",
				Namespace:            "",
				Name:                 "go-function",
				ClassName:            "",
				LogTopic:             "log-topic",
				ProcessingGuarantees: 0,
				UserConfig:           `{"word-of-the-day": "hapax legomenon"}`,
				SecretsMap:           "",
				Runtime:              0,
				AutoAck:              true,
				Parallelism:          0,
				Source: &pb.SourceSpec{
					SubscriptionType: pb.SubscriptionType(0),
					InputSpecs: map[string]*pb.ConsumerSpec{
						"persistent://public/default/topic-01": {
							SchemaType:     "",
							IsRegexPattern: false,
							ReceiverQueueSize: &pb.ConsumerSpec_ReceiverQueueSize{
								Value: 10,
							},
						},
					},
					TimeoutMs:            0,
					SubscriptionName:     "",
					CleanupSubscription:  false,
					SubscriptionPosition: pb.SubscriptionPosition_EARLIEST,
				},
				Sink: &pb.SinkSpec{
					Topic:      "persistent://public/default/topic-02",
					SchemaType: "",
				},
				Resources: &pb.Resources{
					Cpu:  0,
					Ram:  0,
					Disk: 0,
				},
				PackageUrl: "",
				RetryDetails: &pb.RetryDetails{
					MaxMessageRetries: 0,
					DeadLetterTopic:   "",
				},
				RuntimeFlags:         "",
				ComponentType:        0,
				CustomRuntimeOptions: "",
			},
		},
		newInstanceConf(),
	)
}

func TestInstanceConf_GetInstanceName(t *testing.T) {
	instanceConf := newInstanceConf()
	instanceName := instanceConf.getInstanceName()

	assert.Equal(t, "101", instanceName)
}
