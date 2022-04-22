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
package org.apache.pulsar.common.naming;

import com.google.common.collect.Sets;
import java.util.Collections;
import java.util.Set;

/**
 * Encapsulate the parsing of the completeTopicName name.
 */
public class SystemTopicNames {

    /**
     * Local topic name for the namespace events.
     */
    public static final String NAMESPACE_EVENTS_LOCAL_NAME = "__change_events";

    /**
     * Local topic name for the transaction buffer snapshot.
     */
    public static final String TRANSACTION_BUFFER_SNAPSHOT = "__transaction_buffer_snapshot";


    public static final String PENDING_ACK_STORE_SUFFIX = "__transaction_pending_ack";

    public static final String PENDING_ACK_STORE_CURSOR_NAME = "__pending_ack_state";

    /**
     * The set of all local topic names declared above.
     */
    public static final Set<String> EVENTS_TOPIC_NAMES =
            Collections.unmodifiableSet(Sets.newHashSet(NAMESPACE_EVENTS_LOCAL_NAME, TRANSACTION_BUFFER_SNAPSHOT));


    public static final TopicName TRANSACTION_COORDINATOR_ASSIGN = TopicName.get(TopicDomain.persistent.value(),
            NamespaceName.SYSTEM_NAMESPACE, "transaction_coordinator_assign");

    public static final TopicName TRANSACTION_COORDINATOR_LOG = TopicName.get(TopicDomain.persistent.value(),
            NamespaceName.SYSTEM_NAMESPACE, "__transaction_log_");

    public static final TopicName RESOURCE_USAGE_TOPIC = TopicName.get(TopicDomain.non_persistent.value(),
            NamespaceName.SYSTEM_NAMESPACE, "resource-usage");

    public static boolean isEventSystemTopic(TopicName topicName) {
        return EVENTS_TOPIC_NAMES.contains(TopicName.get(topicName.getPartitionedTopicName()).getLocalName());
    }

    public static boolean isTransactionCoordinatorAssign(TopicName topicName) {
        return topicName != null && topicName.toString()
                .startsWith(TRANSACTION_COORDINATOR_ASSIGN.toString());
    }

    public static boolean isTopicPoliciesSystemTopic(String topic) {
        if (topic == null) {
            return false;
        }
        return TopicName.get(topic).getLocalName().equals(NAMESPACE_EVENTS_LOCAL_NAME);
    }

    public static boolean isTransactionInternalName(TopicName topicName) {
        String topic = topicName.toString();
        return topic.startsWith(TRANSACTION_COORDINATOR_ASSIGN.toString())
                || topic.startsWith(TRANSACTION_COORDINATOR_LOG.toString())
                || topic.endsWith(PENDING_ACK_STORE_SUFFIX);
    }

    public static boolean isSystemTopic(TopicName topicName) {
        TopicName nonePartitionedTopicName = TopicName.get(topicName.getPartitionedTopicName());
        return isEventSystemTopic(nonePartitionedTopicName) || isTransactionInternalName(nonePartitionedTopicName);
    }
}
