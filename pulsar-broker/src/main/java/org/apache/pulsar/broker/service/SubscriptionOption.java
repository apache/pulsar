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
package org.apache.pulsar.broker.service;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.Getter;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.common.api.proto.CommandSubscribe;
import org.apache.pulsar.common.api.proto.KeySharedMeta;
import org.apache.pulsar.common.api.proto.KeyValue;

@Getter
@Builder
public class SubscriptionOption {
    private final TransportCnx cnx;
    private String subscriptionName;
    private long consumerId;
    private CommandSubscribe.SubType subType;
    private int priorityLevel;
    private String consumerName;
    private boolean isDurable;
    private MessageId startMessageId;
    private Map<String, String> metadata;
    private boolean readCompacted;
    private CommandSubscribe.InitialPosition initialPosition;
    private long startMessageRollbackDurationSec;
    private boolean replicatedSubscriptionStateArg;
    private KeySharedMeta keySharedMeta;
    private Optional<Map<String, String>> subscriptionProperties;

    public static Optional<Map<String, String>> getPropertiesMap(List<KeyValue> list) {
        if (list == null) {
            return Optional.of(Collections.emptyMap());
        }
        return Optional.of(list.stream().collect(Collectors.toMap(
                KeyValue::getKey, KeyValue::getValue, (key1, key2) -> key1)));
    }
}
