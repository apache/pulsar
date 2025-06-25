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
package org.apache.pulsar.common.naming;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.StringUtils;

/**
 * {@link TopicName} is over heavy in many simple cases. This util class provides many methods to perform topic name
 * conversions quickly without a global cache.
 */
public class TopicNameUtils {

    /**
     * Convert a topic name to a full topic name.
     * In Pulsar, a full topic name is "<domain>://<tenant>/<namespace>/<local-topic>" (v2) or
     * "<domain>://<tenant>/<cluster>/<namespace>/<local-topic>" (v1). However, for convenient, it's allowed for clients
     * to pass a short topic name with v2 format:
     * - "<local-topic>", which represents "persistent://public/default/<local-topic>"
     * - "<tenant>/<namespace>/<local-topic>, which represents "persistent://<tenant>/<namespace>/<local-topic>"
     *
     * @param topic the topic name from client
     * @return the full topic name.
     */
    public static String toFullTopicName(String topic) {
        final int index = topic.indexOf("://");
        if (index > 0) {
            TopicDomain.getEnum(topic.substring(0, index));
            final List<String> parts = splitBySlash(topic, 4);
            if (parts.size() != 3 && parts.size() != 4) {
                throw new IllegalArgumentException(topic + " is invalid");
            }
            if (parts.size() == 3) {
                NamespaceName.validateNamespaceName(parts.get(0), parts.get(1));
                if (StringUtils.isBlank(parts.get(2))) {
                    throw new IllegalArgumentException(topic + " has blank local topic");
                }
            } else {
                NamespaceName.validateNamespaceName(parts.get(0), parts.get(1), parts.get(2));
                if (StringUtils.isBlank(parts.get(3))) {
                    throw new IllegalArgumentException(topic + " has blank local topic");
                }
            }
            return topic; // it's a valid full topic name
        } else {
            List<String> parts = splitBySlash(topic, 0);
            if (parts.size() != 1 && parts.size() != 3) {
                throw new IllegalArgumentException(topic + " is invalid");
            }
            if (parts.size() == 1) {
                if (StringUtils.isBlank(parts.get(0))) {
                    throw new IllegalArgumentException(topic + " has blank local topic");
                }
                return "persistent://public/default/" + parts.get(0);
            } else {
                NamespaceName.validateNamespaceName(parts.get(0), parts.get(1));
                if (StringUtils.isBlank(parts.get(2))) {
                    throw new IllegalArgumentException(topic + " has blank local topic");
                }
                return "persistent://" + topic;
            }
        }
    }

    private static List<String> splitBySlash(String topic, int limit) {
        final List<String> tokens = new ArrayList<>(3);
        final int loopCount = (limit <= 0) ? Integer.MAX_VALUE : limit - 1;
        int beginIndex = 0;
        for (int i = 0; i < loopCount; i++) {
            final int endIndex = topic.indexOf('/', beginIndex);
            if (endIndex < 0) {
                tokens.add(topic.substring(beginIndex));
                return tokens;
            } else if (endIndex > beginIndex) {
                tokens.add(topic.substring(beginIndex, endIndex));
            } else {
                throw new IllegalArgumentException("Invalid topic name " + topic);
            }
            beginIndex = endIndex + 1;
        }
        if (beginIndex >= topic.length()) {
            throw new IllegalArgumentException("Invalid topic name " + topic);
        }
        tokens.add(topic.substring(beginIndex));
        return tokens;
    }
}
