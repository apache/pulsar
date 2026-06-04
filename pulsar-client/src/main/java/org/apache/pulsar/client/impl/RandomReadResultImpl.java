/*
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
package org.apache.pulsar.client.impl;

import java.util.List;
import java.util.Optional;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageInvisibleReason;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.RandomReadResult;

public class RandomReadResultImpl<T> implements RandomReadResult<T> {
    private final MessageId messageId;
    private final List<Message<T>> messages;
    private final PulsarClientException.MessageInvisibleException invisibleException;

    private RandomReadResultImpl(MessageId messageId, List<Message<T>> messages,
                                 PulsarClientException.MessageInvisibleException invisibleException) {
        this.messageId = messageId;
        this.messages = messages;
        this.invisibleException = invisibleException;
    }

    public static <T> RandomReadResultImpl<T> visible(MessageId messageId, List<Message<T>> messages) {
        return new RandomReadResultImpl<>(messageId, List.copyOf(messages), null);
    }

    public static <T> RandomReadResultImpl<T> invisible(MessageId messageId, MessageInvisibleReason reason) {
        return new RandomReadResultImpl<>(messageId, null,
                new PulsarClientException.MessageInvisibleException(messageId, reason));
    }

    @Override
    public MessageId getMessageId() {
        return messageId;
    }

    @Override
    public boolean isVisible() {
        return invisibleException == null;
    }

    @Override
    public List<Message<T>> getMessages() throws PulsarClientException.MessageInvisibleException {
        if (invisibleException != null) {
            throw invisibleException;
        }
        return messages;
    }

    @Override
    public Optional<PulsarClientException.MessageInvisibleException> getInvisibleException() {
        return Optional.ofNullable(invisibleException);
    }
}
