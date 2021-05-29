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
package org.apache.pulsar.functions.api.examples;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;

/**
 * Merge various schemas data to a topic.
 */
@Slf4j
public class MergeTopicFunction implements Function<GenericRecord, byte[]> {

    @Override
    public byte[] process(GenericRecord genericRecord, Context context) throws Exception {
        if (context.getCurrentRecord().getMessage().isPresent()) {
            Message<?> msg =  context.getCurrentRecord().getMessage().get();
            if (!msg.getReaderSchema().isPresent()) {
                log.warn("The reader schema is null.");
                return null;
            }
            log.info("process message with reader schema {}", msg.getReaderSchema().get());
            TypedMessageBuilder<byte[]> messageBuilder =
                    context.newOutputMessage(context.getOutputTopic(),
                            Schema.AUTO_PRODUCE_BYTES(msg.getReaderSchema().get()));

            messageBuilder
                    .value(msg.getData())
                    .property("__original_topic", msg.getTopicName())
                    .property("__publish_time", String.valueOf(msg.getPublishTime()))
                    .property("__sequence_id", String.valueOf(msg.getSequenceId()))
                    .property("__producer_name", msg.getProducerName());

            if (msg.getKeyBytes() != null)  {
                messageBuilder.keyBytes(msg.getKeyBytes());
            }

            if (msg.getEventTime() > 0) {
                messageBuilder.eventTime(msg.getEventTime());
            }

            if (!msg.getProperties().isEmpty()) {
                messageBuilder.properties(msg.getProperties());
            }

            messageBuilder.send();
            log.info("send message successfully");
        } else {
            log.warn("context current record message is not present.");
        }
        return null;
    }
}
