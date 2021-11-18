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
package org.apache.pulsar.common.events;

import com.google.common.collect.Sets;
import java.util.Collections;
import java.util.Set;
import org.apache.pulsar.common.naming.TopicName;

/**
 * System topic names for each {@link EventType}.
 */
public class EventsTopicNames {

    /**
     * Local topic name for the namespace events.
     */
    public static final String NAMESPACE_EVENTS_LOCAL_NAME = "__change_events";

    /**
     * Local topic name for the transaction buffer snapshot.
     */
    public static final String TRANSACTION_BUFFER_SNAPSHOT = "__transaction_buffer_snapshot";

    /**
     * The set of all local topic names declared above.
     */
    public static final Set<String> EVENTS_TOPIC_NAMES =
            Collections.unmodifiableSet(Sets.newHashSet(NAMESPACE_EVENTS_LOCAL_NAME, TRANSACTION_BUFFER_SNAPSHOT));

    public static boolean checkTopicIsEventsNames(TopicName topicName) {
        return EVENTS_TOPIC_NAMES.contains(TopicName.get(topicName.getPartitionedTopicName()).getLocalName());
    }

    public static boolean checkTopicIsTransactionCoordinatorAssign(TopicName topicName) {
        return topicName != null && topicName.toString()
                .startsWith(TopicName.TRANSACTION_COORDINATOR_ASSIGN.toString());
    }

    public static boolean isTopicPoliciesSystemTopic(String topic) {
        if (topic == null) {
            return false;
        }
        return TopicName.get(topic).getLocalName().equals(NAMESPACE_EVENTS_LOCAL_NAME);
    }
}
