/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package pf

import (
	"fmt"
	"time"

	"github.com/apache/pulsar/pulsar-function-go/conf"
	"github.com/apache/pulsar/pulsar-function-go/pb"
)

// This is the config passed to the Golang Instance. Contains all the information
// passed to run functions
type InstanceConf struct {
	InstanceID       int
	FuncID           string
	FuncVersion      string
	FuncDetails      pb.FunctionDetails
	MaxBufTuples     int
	Port             int
	ClusterName      string
	PulsarServiceURL string
	KillAfterIdleMs  time.Duration
}

func NewInstanceConf() *InstanceConf {
	config := &conf.Conf{}
	cfg := config.GetConf()
	instanceConf := &InstanceConf{
		InstanceID:       cfg.InstanceID,
		FuncID:           cfg.FuncID,
		FuncVersion:      cfg.FuncVersion,
		MaxBufTuples:     cfg.MaxBufTuples,
		Port:             cfg.Port,
		ClusterName:      cfg.ClusterName,
		PulsarServiceURL: cfg.PulsarServiceURL,
		KillAfterIdleMs:  cfg.KillAfterIdleMs,
		FuncDetails: pb.FunctionDetails{
			Name:    cfg.Name,
			AutoAck: cfg.AutoACK,
			Source: &pb.SourceSpec{
				InputSpecs: map[string]*pb.ConsumerSpec{
					cfg.SourceSpecTopic: {
						IsRegexPattern: cfg.IsRegexPatternSubscription,
						ReceiverQueueSize: &pb.ConsumerSpec_ReceiverQueueSize{
							Value: cfg.ReceiverQueueSize,
						},
					},
				},
			},
			Sink: &pb.SinkSpec{
				Topic: cfg.SinkSpecTopic,
			},
			Resources:    &pb.Resources{},
			RetryDetails: &pb.RetryDetails{},
		},
	}
	return instanceConf
}

func (ic *InstanceConf) GetInstanceName() string {
	return "" + fmt.Sprintf("%d", ic.InstanceID)
}
