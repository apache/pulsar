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
package org.apache.pulsar.functions.instance.processors;

import java.util.concurrent.LinkedBlockingDeque;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageBuilder;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.functions.instance.InputMessage;
import org.apache.pulsar.functions.instance.producers.AbstractOneOuputTopicProducers;
import org.apache.pulsar.functions.proto.Function.FunctionConfig;

/**
 * A message processor that process messages at-most-once.
 */
@Slf4j
class AtMostOnceProcessor extends MessageProcessorBase {

    private Producer producer;

    AtMostOnceProcessor(PulsarClient client,
                        FunctionConfig functionConfig,
                        SubscriptionType subType,
                        LinkedBlockingDeque<InputMessage> processQueue) {
        super(client, functionConfig, subType, processQueue);
    }

    @Override
    protected void postReceiveMessage(InputMessage message) {
        super.postReceiveMessage(message);
        if (functionConfig.getAutoAck()) {
            message.ack();
        }
    }

    @Override
    protected void initializeOutputProducer(String outputTopic) throws Exception {
        producer = AbstractOneOuputTopicProducers.createProducer(client, outputTopic);
    }

    @Override
    public void sendOutputMessage(InputMessage inputMsg, MessageBuilder outputMsgBuilder) {
        if (null == outputMsgBuilder) {
            return;
        }

        Message outputMsg = outputMsgBuilder.build();
        producer.sendAsync(outputMsg);
    }

    @Override
    public void close() {
        super.close();
        if (null != producer) {
            try {
                producer.close();
            } catch (PulsarClientException e) {
                log.warn("Fail to close producer for processor {}", functionConfig.getOutput(), e);
            }
        }
    }
}
