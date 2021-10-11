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
package org.apache.pulsar.client.processor;

import io.netty.buffer.ByteBuf;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.MessagePayloadContext;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessagePayload;
import org.apache.pulsar.client.api.MessagePayloadFactory;
import org.apache.pulsar.client.api.MessagePayloadProcessor;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.MessagePayloadUtils;

@Slf4j
public class CustomBatchPayloadProcessor implements MessagePayloadProcessor {

    @Override
    public <T> void process(MessagePayload payload, MessagePayloadContext context, Schema<T> schema,
                            Consumer<Message<T>> messageConsumer) throws Exception {
        final String value = context.getProperty(CustomBatchFormat.KEY);
        if (value == null || !value.equals(CustomBatchFormat.VALUE)) {
            DEFAULT.process(payload, context, schema, messageConsumer);
            return;
        }

        final ByteBuf buf = MessagePayloadUtils.convertToByteBuf(payload);
        try {
            final int numMessages = CustomBatchFormat.readMetadata(buf).getNumMessages();
            for (int i = 0; i < numMessages; i++) {
                final MessagePayload singlePayload =
                        MessagePayloadFactory.DEFAULT.wrap(CustomBatchFormat.readMessage(buf));
                try {
                    messageConsumer.accept(
                            context.getMessageAt(i, numMessages, singlePayload, false, schema));
                } finally {
                    singlePayload.release();
                }
            }
        } finally {
            buf.release();
        }
    }
}
