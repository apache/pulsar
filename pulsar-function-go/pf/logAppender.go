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
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	log "github.com/apache/pulsar/pulsar-function-go/logutil"
)

type LogAppender struct {
	pulsarClient pulsar.Client
	logTopic     string
	fqn          string
	producer     pulsar.Producer
}

func NewLogAppender(client pulsar.Client, logTopic, fqn string) *LogAppender {
	logAppender := &LogAppender{
		pulsarClient: client,
		logTopic:     logTopic,
		fqn:          fqn,
	}
	return logAppender
}

func (la *LogAppender) Start() error {
	producer, err := la.pulsarClient.CreateProducer(pulsar.ProducerOptions{
		Topic:                   la.logTopic,
		CompressionType:         pulsar.LZ4,
		BatchingMaxPublishDelay: 100 * time.Millisecond,
		Properties: map[string]string{
			"function": la.fqn,
		},
	})
	if err != nil {
		log.Errorf("create producer error:%s", err.Error())
		return err
	}
	la.producer = producer
	return nil
}

func (la *LogAppender) Append(logByte []byte) {
	ctx := context.Background()
	asyncMsg := pulsar.ProducerMessage{
		Payload: logByte,
	}
	la.producer.SendAsync(ctx, &asyncMsg, func(id pulsar.MessageID, message *pulsar.ProducerMessage, err error) {
		if err != nil {
			log.Fatal(err)
		}
	})
}

func (la *LogAppender) GetName() string {
	return la.fqn
}

func (la *LogAppender) Stop() {
	la.producer.Close()
	la.producer = nil
}
