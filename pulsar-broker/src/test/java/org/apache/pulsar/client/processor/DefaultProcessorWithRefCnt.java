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

import java.util.function.Consumer;
import lombok.Getter;
import org.apache.pulsar.client.api.MessagePayloadContext;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessagePayload;
import org.apache.pulsar.client.api.MessagePayloadProcessor;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.MessagePayloadImpl;

/**
 * The processor for Pulsar format messages and maintains a total reference count.
 *
 * It's used to verify {@link MessagePayloadContext#getMessageAt} and {@link MessagePayloadContext#asSingleMessage} have release the
 * ByteBuf successfully.
 */
public class DefaultProcessorWithRefCnt implements MessagePayloadProcessor {

    @Getter
    int totalRefCnt = 0;

    @Override
    public <T> void process(MessagePayload payload, MessagePayloadContext context, Schema<T> schema,
                            Consumer<Message<T>> messageConsumer) throws Exception {
        MessagePayloadProcessor.DEFAULT.process(payload, context, schema, messageConsumer);
        totalRefCnt += ((MessagePayloadImpl) payload).getByteBuf().refCnt();
    }
}
