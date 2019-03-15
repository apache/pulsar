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

	"github.com/apache/pulsar/pulsar-function-go/conf"
	"github.com/apache/pulsar/pulsar-function-go/pb"
)

// This is the config passed to the Golang Instance. Contains all the information
// passed to run functions
type InstanceConf struct {
	InstanceID   int
	FuncID       string
	FuncVersion  string
	FuncDetails  pb.FunctionDetails
	MaxBufTuples int
	Port         int
	ClusterName  string
}

func NewInstanceConf() *InstanceConf {
	conf := &conf.Conf{}
	cfg := conf.GetConf()
	instanceConf := &InstanceConf{
		InstanceID:   cfg.InstanceID,
		FuncID:       cfg.FuncID,
		FuncVersion:  cfg.FuncVersion,
		MaxBufTuples: cfg.MaxBufTuples,
		Port:         cfg.Port,
		ClusterName:  cfg.ClusterName,
		FuncDetails: pb.FunctionDetails{
			Source: &pb.SourceSpec{
				InputSpecs: map[string]*pb.ConsumerSpec{
					cfg.InputSpecsTopic: {
						IsRegexPattern: cfg.IsRegexPattern,
						ReceiverQueueSize: &pb.ConsumerSpec_ReceiverQueueSize{
							Value: cfg.ReceiverQueueVal,
						},
					},
				},
			},
			Sink: &pb.SinkSpec{
				Topic: "topic2",
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
