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

import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import java.util.Map;
import java.util.Optional;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.common.api.EncryptionContext;
import org.apache.pulsar.io.core.Record;

@Data
@Builder
@Getter
@ToString
@EqualsAndHashCode
public class PulsarRecord<T> implements Record<T> {

    private String partitionId;
    private long recordSequence;
    private T value;
    private MessageId messageId;
    private String topicName;
    private Map<String, String> properties;
    private Optional<EncryptionContext> encryptionCtx = Optional.empty();
    private Runnable failFunction;
    private Runnable ackFunction;

    @Override
    public void ack() {
        this.ackFunction.run();
    }

    @Override
    public void fail() {
        this.failFunction.run();
    }
}
