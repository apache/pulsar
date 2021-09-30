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
package org.apache.pulsar.client.api;

import java.util.function.Consumer;

/**
 * The processor to process a message payload.
 *
 * It's responsible to convert the raw buffer to some messages, then trigger some callbacks so that consumer can consume
 * these messages and handle the exception if it existed.
 *
 * The most important part is to decode the raw buffer. After that, we can call
 * {@link MessagePayloadContext#getMessageAt} or {@link MessagePayloadContext#asSingleMessage} to construct
 * {@link Message} for consumer to consume. Since we need to pass the {@link MessagePayload} object to these methods, we
 * can use {@link MessagePayloadFactory#DEFAULT} to create it or just reuse the payload argument.
 */
public interface MessagePayloadProcessor {

    /**
     * Process the message payload.
     *
     * @param payload the payload whose underlying buffer is a Netty ByteBuf
     * @param context the message context that contains the message format information and methods to create a message
     * @param schema the message's schema
     * @param messageConsumer the callback to consume each message
     * @param <T>
     * @throws Exception
     */
    <T> void process(MessagePayload payload,
                     MessagePayloadContext context,
                     Schema<T> schema,
                     Consumer<Message<T>> messageConsumer) throws Exception;

    // The default processor for Pulsar format payload. It should be noted getNumMessages() and isBatch() methods of
    // EntryContext only work for Pulsar format. For other formats, the message metadata might be stored in the payload.
    MessagePayloadProcessor DEFAULT = new MessagePayloadProcessor() {

        @Override
        public <T> void process(MessagePayload payload,
                                MessagePayloadContext context,
                                Schema<T> schema,
                                Consumer<Message<T>> messageConsumer) {
            if (context.isBatch()) {
                final int numMessages = context.getNumMessages();
                for (int i = 0; i < numMessages; i++) {
                    messageConsumer.accept(context.getMessageAt(i, numMessages, payload, true, schema));
                }
            } else {
                messageConsumer.accept(context.asSingleMessage(payload, schema));
            }
        }
    };
}
