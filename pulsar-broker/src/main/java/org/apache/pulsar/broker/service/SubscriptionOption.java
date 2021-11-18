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
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.Data;
import org.apache.pulsar.broker.service.TransportCnx;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.common.api.proto.CommandSubscribe;
import org.apache.pulsar.common.api.proto.KeyLongValue;
import org.apache.pulsar.common.api.proto.KeySharedMeta;

@Data
@Builder
public class SubscriptionOption {
    final TransportCnx cnx;
    String subscriptionName;
    long consumerId;
    CommandSubscribe.SubType subType;
    int priorityLevel;
    String consumerName;
    boolean isDurable;
    MessageId startMessageId;
    Map<String, String> metadata;
    boolean readCompacted;
    CommandSubscribe.InitialPosition initialPosition;
    long startMessageRollbackDurationSec;
    boolean replicatedSubscriptionStateArg;
    KeySharedMeta keySharedMeta;
    Map<String, Long> subscriptionProperties;

    public static Map<String, Long> getPropertiesMap(List<KeyLongValue> list) {
        if (list == null) {
            return Collections.emptyMap();
        }
        return list.stream().collect(Collectors.toMap(
                KeyLongValue::getKey, KeyLongValue::getValue, (key1, key2) -> key1));
    }
}
