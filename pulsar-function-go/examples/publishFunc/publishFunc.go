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

package main

import (
	"context"
	"errors"
	"log"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/apache/pulsar/pulsar-function-go/pf"
)

func PublishFunc(ctx context.Context, in []byte) error {
	fctx, ok := pf.FromContext(ctx)
	if !ok {
		return errors.New("get Go Functions Context error")
	}

	publishTopic := "publish-topic"
	output := append(in, 110)

	producer := fctx.NewOutputMessage(publishTopic)
	msgID, err := producer.Send(ctx, &pulsar.ProducerMessage{
		Payload: output,
	})
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("The output message ID is: %+v", msgID)
	return nil
}

func main() {
	pf.Start(PublishFunc)
}