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
package org.apache.pulsar.functions.source;

import java.util.Map;
import java.util.Optional;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.common.api.EncryptionContext;
import org.apache.pulsar.functions.utils.Utils;
import org.apache.pulsar.io.core.Record;

@Builder
@Getter
@ToString
@EqualsAndHashCode
public class PulsarRecord<T> implements Record<T> {

    private final String topicName;
    private final int partition;

    // TODO: When we switch to schema for functions, we should just rely on the message object value
    private final T value;
    private final Message<byte[]> message;

    private final Runnable failFunction;
    private final Runnable ackFunction;

    @Override
    public Optional<String> getKey() {
        if (message.hasKey()) {
            return Optional.of(message.getKey());
        } else {
            return Optional.empty();
        }
    }

    @Override
    public Optional<String> getPartitionId() {
        return Optional.of(String.format("%s-%s", topicName, partition));
    }

    @Override
    public Optional<Long> getRecordSequence() {
        return Optional.of(Utils.getSequenceId(message.getMessageId()));
    }

    /**
     * Retrieves encryption-context that is attached to record.
     *
     * @return {@link Optional}<{@link EncryptionContext}>
     */
    public Optional<EncryptionContext> getEncryptionCtx() {
        return message.getEncryptionCtx();
    }

    @Override
    public Map<String, String> getProperties() {
        return message.getProperties();
    }

    public MessageId getMessageId() {
        return message.getMessageId();
    }

    @Override
    public void ack() {
        this.ackFunction.run();
    }

    @Override
    public void fail() {
        this.failFunction.run();
    }
}
