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
package org.apache.pulsar.broker.systopic;

import lombok.extern.slf4j.Slf4j;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.pulsar.broker.transaction.pendingack.impl.MLPendingAckStore;
import org.apache.pulsar.common.events.EventsTopicNames;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;

@Slf4j
public class SystemTopicNameManager {
    private static final Map<String, SystemTopicNamePolicy> systemTopics = new ConcurrentHashMap<>(24);

    public static final String BROKER_NS_HEALTH_CHECK_NAME = "healthcheck";

    static {
        register(BROKER_NS_HEALTH_CHECK_NAME);
        register(MLPendingAckStore.PENDING_ACK_STORE_SUFFIX, SystemTopicNamePolicy.SUFFIX);
    }

    public static void register(TopicName topicName) {
        TopicName topicNameWithOutPartitioned = TopicName.get(topicName.getPartitionedTopicName());
        register(topicNameWithOutPartitioned.getLocalName());
    }

    public static void register(String topicName) {
        register(topicName, SystemTopicNamePolicy.FULL_QUALITY);
    }

    public static void register(String topicName, SystemTopicNamePolicy systemTopicNamePolicy) {
        SystemTopicNamePolicy prev = systemTopics.put(topicName, systemTopicNamePolicy);
        if (log.isDebugEnabled()) {
            log.debug("Register system topic name {}", topicName);
        }
        if (prev != null) {
            log.warn("Register system topic name {} more than once.", topicName);
        }
    }

    public static boolean isSystemTopic(TopicName topicName) {
        if (topicName.getNamespaceObject().equals(NamespaceName.SYSTEM_NAMESPACE)) {
            return true;
        }
        TopicName topicNameWithOutPartitioned = TopicName.get(topicName.getPartitionedTopicName());
        // event system topic names
        if (EventsTopicNames.checkTopicIsEventsNames(topicNameWithOutPartitioned)) {
            return true;
        }
        return internalJudgeSystemTopic(topicNameWithOutPartitioned.getLocalName());
    }

    private static boolean internalJudgeSystemTopic(String topicName) {
        for (Map.Entry<String, SystemTopicNamePolicy> entry : systemTopics.entrySet()) {
            String possibleKey = entry.getKey();
            SystemTopicNamePolicy namePolicy = entry.getValue();
            if (!topicName.contains(possibleKey)) {
                continue;
            }
            switch (namePolicy) {
                case FULL_QUALITY:
                    return topicName.equals(possibleKey);
                case PREFIX:
                    return topicName.startsWith(possibleKey);
                case SUFFIX:
                    return topicName.endsWith(possibleKey);
            }
        }
        return false;
    }

}
