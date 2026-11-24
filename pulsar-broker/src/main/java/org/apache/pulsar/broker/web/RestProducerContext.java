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

package org.apache.pulsar.broker.web;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.pulsar.broker.service.TopicEventsListener;
import org.apache.pulsar.common.naming.TopicName;

/**
 * Per-broker cache of topic partitions that this broker currently owns for the REST producer flow.
 *
 * <p>Used by {@link org.apache.pulsar.broker.rest.TopicsBase} to back the REST API for producing messages to
 * Pulsar topics. When a REST publish request arrives, {@code TopicsBase} consults this context to decide whether
 * the local broker already owns (some partitions of) the target topic. If it does, the publish proceeds directly
 * against the in-process topic instance; otherwise a namespace lookup is performed and the request is redirected
 * to the owning broker. After a successful lookup that resolves to this broker, {@code TopicsBase} eagerly
 * registers the topic via {@link #addTopic(String)} so that subsequent publishes do not race the asynchronous
 * {@link TopicEvent#LOAD} listener notification.
 *
 * <p>Ownership is tracked as a map from partitioned-topic name to the set of partition indexes owned locally.
 * Entries are removed when a topic is unloaded: this class implements {@link TopicEventsListener} and reacts to
 * {@link TopicEvent#UNLOAD} events.
 */
public class RestProducerContext implements TopicEventsListener {
    private final Map<String, Set<Integer>> owningTopics = new ConcurrentHashMap<>();

    public boolean isTopicOwned(String topic) {
        return owningTopics.containsKey(topic);
    }

    @Override
    public void handleEvent(String topicName, TopicEvent event, EventStage stage, Throwable t) {
        // remove topic from owning topics when it's unloaded
        if (event == TopicEvent.UNLOAD && stage == EventStage.SUCCESS) {
            addOrRemoveTopic(topicName, true);
        }
    }

    public void addTopic(String topicName) {
        addOrRemoveTopic(topicName, false);
    }

    private void addOrRemoveTopic(String topicName, boolean remove) {
        TopicName topic = TopicName.get(topicName);
        owningTopics.compute(topic.getPartitionedTopicName(), (k, partitionSet) -> {
            if (remove) {
                if (partitionSet != null) {
                    partitionSet.remove(topic.getPartitionIndex());
                }
            } else {
                if (partitionSet == null) {
                    partitionSet = ConcurrentHashMap.newKeySet();
                }
                partitionSet.add(topic.getPartitionIndex());
            }
            return partitionSet == null || partitionSet.isEmpty() ? null : partitionSet;
        });
    }

    public boolean containsTopicPartition(String topic, int partition) {
        return owningTopics.getOrDefault(topic, Set.of()).contains(partition);
    }

    public List<Integer> getPartitionIndexes(String topic) {
        return owningTopics.getOrDefault(topic, Set.of()).stream().toList();
    }
}
