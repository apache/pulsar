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
package org.apache.pulsar.common.lookup;

import com.google.re2j.Pattern;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import lombok.Getter;
import lombok.ToString;
import org.apache.pulsar.common.api.proto.CommandGetTopicsOfNamespace;
import org.apache.pulsar.common.api.proto.CommandGetTopicsOfNamespaceResponse;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.topics.TopicList;

/***
 * A value object.
 * - The response of HTTP API "admin/v2/namespaces/{domain}/topics" is a topic(non-partitioned topic or partitions)
 *   array. It will be wrapped to "topics: {topic array}, topicsHash: null, filtered: false, changed: true".
 * - The response of binary API {@link CommandGetTopicsOfNamespace} is a {@link CommandGetTopicsOfNamespaceResponse},
 *   it will be transferred to a {@link GetTopicsResult}.
 * See more details https://github.com/apache/pulsar/pull/14804.
 */
@ToString
public class GetTopicsResult {

    /**
     * Non-partitioned topics, and topic partitions of partitioned topics.
     */
    @Getter
    private final List<String> nonPartitionedOrPartitionTopics;

    /**
     * The topics have been filtered by Broker using a regexp. Otherwise, the client should do a client-side filter.
     * There are three cases that brokers will not filter the topics:
     * 1. the lookup service is typed HTTP lookup service, the HTTP API has not implemented this feature yet.
     * 2. the broker does not support this feature(in other words, its version is lower than "2.11.0").
     * 3. the input param "topicPattern" is too long than the broker config "subscriptionPatternMaxLength".
     */
    @Getter
    private final boolean filtered;

    /**
     * The topics hash that was calculated by {@link TopicList#calculateHash(List)}. The param topics that will be used
     * to calculate the hash code is only contains the topics that has been filtered.
     * Note: It is always "null" if broker did not filter the topics when calling the API
     * "LookupService.getTopicsUnderNamespace"(in other words, {@link #filtered} is false).
     */
    @Getter
    private final String topicsHash;

    /**
     * The topics hash has changed after compare with the input param "topicsHash" when calling
     * "LookupService.getTopicsUnderNamespace".
     * Note: It is always set "true" if the input param "topicsHash" that used to call
     * "LookupService.getTopicsUnderNamespace" is null or the "LookupService" is "HttpLookupService".
     */
    @Getter
    private final boolean changed;

    /**
     * Partitioned topics and non-partitioned topics.
     * In other words, there is no topic partitions of partitioned topics in this list.
     * Note: it is not a field of the response of "LookupService.getTopicsUnderNamespace", it is generated in
     * client-side memory.
     */
    private volatile List<String> topics;

    /**
     * This constructor is used for binary API.
     */
    public GetTopicsResult(List<String> nonPartitionedOrPartitionTopics, String topicsHash, boolean filtered,
                           boolean changed) {
        this.nonPartitionedOrPartitionTopics = nonPartitionedOrPartitionTopics;
        this.topicsHash = topicsHash;
        this.filtered = filtered;
        this.changed = changed;
    }

    /**
     * This constructor is used for HTTP API.
     */
    public GetTopicsResult(String[] nonPartitionedOrPartitionTopics) {
        this(Arrays.asList(nonPartitionedOrPartitionTopics), null, false, true);
    }

    public List<String> getTopics() {
        if (topics != null) {
            return topics;
        }
        synchronized (this) {
            if (topics != null) {
                return topics;
            }
            // Group partitioned topics.
            List<String> grouped = new ArrayList<>();
            for (String topic : nonPartitionedOrPartitionTopics) {
                String partitionedTopic = TopicName.get(topic).getPartitionedTopicName();
                if (!grouped.contains(partitionedTopic)) {
                    grouped.add(partitionedTopic);
                }
            }
            topics = grouped;
            return topics;
        }
    }

    public GetTopicsResult filterTopics(Pattern topicsPattern) {
        List<String> topicsFiltered = TopicList.filterTopics(getTopics(), topicsPattern);
        // If nothing changed.
        if (topicsFiltered.equals(getTopics())) {
            GetTopicsResult newObj = new GetTopicsResult(nonPartitionedOrPartitionTopics, null, true, true);
            newObj.topics = topics;
            return newObj;
        }
        // Filtered some topics.
        Set<String> topicsFilteredSet = new HashSet<>(topicsFiltered);
        List<String> newTps = new ArrayList<>();
        for (String tp: nonPartitionedOrPartitionTopics) {
            if (topicsFilteredSet.contains(TopicName.get(tp).getPartitionedTopicName())) {
                newTps.add(tp);
            }
        }
        GetTopicsResult newObj = new GetTopicsResult(newTps, null, true, true);
        newObj.topics = topicsFiltered;
        return newObj;
    }
}
