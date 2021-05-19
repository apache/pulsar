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
package org.apache.pulsar.client.impl;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import lombok.Getter;
import org.apache.pulsar.client.api.MessageId;

/**
 * A MessageId implementation that contains a map of <partitionName, MessageId>.
 * This is useful when MessageId is need for partition/multi-topics/pattern consumer.
 * e.g. seek(), ackCumulative(), getLastMessageId().
 */
public class MultiMessageIdImpl implements MessageId {
    @Getter
    private Map<String, MessageId> map;

    MultiMessageIdImpl(Map<String, MessageId> map) {
        this.map = map;
    }

    // TODO: Add support for Serialization and Deserialization
    //  https://github.com/apache/pulsar/issues/4940
    @Override
    public byte[] toByteArray() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int hashCode() {
        return Objects.hash(map);
    }

    // If all messageId in map are same size, and all bigger/smaller than the other, return valid value.
    @Override
    public int compareTo(MessageId o) {
        if (!(o instanceof MultiMessageIdImpl)) {
            throw new IllegalArgumentException(
                "expected MultiMessageIdImpl object. Got instance of " + o.getClass().getName());
        }

        MultiMessageIdImpl other = (MultiMessageIdImpl) o;
        Map<String, MessageId> otherMap = other.getMap();

        if ((map == null || map.isEmpty()) && (otherMap == null || otherMap.isEmpty())) {
            return 0;
        }

        if (otherMap == null || map == null || otherMap.size() != map.size()) {
            throw new IllegalArgumentException("Current size and other size not equals");
        }

        int result = 0;
        for (Entry<String, MessageId> entry : map.entrySet()) {
            MessageId otherMessage = otherMap.get(entry.getKey());
            if (otherMessage == null) {
                throw new IllegalArgumentException(
                    "Other MessageId not have topic " + entry.getKey());
            }

            int currentResult = entry.getValue().compareTo(otherMessage);
            if (result == 0) {
                result = currentResult;
            } else if (currentResult == 0) {
                continue;
            } else if (result != currentResult) {
                throw new IllegalArgumentException(
                    "Different MessageId in Map get different compare result");
            } else {
                continue;
            }
        }

        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof MultiMessageIdImpl)) {
            throw new IllegalArgumentException(
                "expected MultiMessageIdImpl object. Got instance of " + obj.getClass().getName());
        }

        MultiMessageIdImpl other = (MultiMessageIdImpl) obj;

        try {
            return compareTo(other) == 0;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }
}
