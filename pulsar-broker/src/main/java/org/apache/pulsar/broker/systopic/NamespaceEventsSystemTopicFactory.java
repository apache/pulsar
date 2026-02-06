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
package org.apache.pulsar.broker.systopic;

import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.SystemTopicTxnBufferSnapshotService;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.common.events.EventType;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.SystemTopicNames;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NamespaceEventsSystemTopicFactory {

    private final PulsarClient client;

    public NamespaceEventsSystemTopicFactory(PulsarClient client) {
        this.client = client;
    }

    public TopicPoliciesSystemTopicClient createTopicPoliciesSystemTopicClient(NamespaceName namespaceName) {
        TopicName topicName = getEventsTopicName(namespaceName);
        log.info("Create topic policies system topic client {}", topicName.toString());
        return new TopicPoliciesSystemTopicClient(client, topicName);
    }

    public static TopicName getEventsTopicName(NamespaceName namespaceName) {
        return TopicName.get(TopicDomain.persistent.value(), namespaceName,
                SystemTopicNames.NAMESPACE_EVENTS_LOCAL_NAME);
    }

    public static CompletableFuture<Boolean> checkSystemTopicExists(NamespaceName namespaceName, EventType eventType,
                                                             PulsarService pulsar) {
        // To check whether partitioned topic exists.
        // Instead of checking partitioned metadata, we check the first partition, because there is a case
        // does not work if we choose checking partitioned metadata.
        // The case's details:
        // 1. Start 2 clusters: c1 and c2.
        // 2. Enable replication between c1 and c2 with a global ZK.
        // 3. The partitioned metadata was shared using by c1 and c2.
        // 4. Pulsar only delete partitions when the topic is deleting from c1, because c2 is still using
        //    partitioned metadata.
        TopicName topicName = getSystemTopicName(namespaceName, eventType);
        CompletableFuture<Boolean> nonPartitionedExists =
                pulsar.getPulsarResources().getTopicResources().persistentTopicExists(topicName);
        CompletableFuture<Boolean> partition0Exists =
                pulsar.getPulsarResources().getTopicResources().persistentTopicExists(topicName.getPartition(0));
        return nonPartitionedExists.thenCombine(partition0Exists, (a, b) -> a | b);
    }

    public <T> TransactionBufferSnapshotBaseSystemTopicClient<T> createTransactionBufferSystemTopicClient(
            TopicName systemTopicName, SystemTopicTxnBufferSnapshotService<T>
            systemTopicTxnBufferSnapshotService, Class<T> schemaType) {
        log.info("Create transaction buffer snapshot client, topicName : {}", systemTopicName.toString());
        return new TransactionBufferSnapshotBaseSystemTopicClient(client, systemTopicName,
                systemTopicTxnBufferSnapshotService, schemaType);
    }

    public static TopicName getSystemTopicName(NamespaceName namespaceName, EventType eventType) {
        return switch (eventType) {
            case TOPIC_POLICY -> TopicName.get(TopicDomain.persistent.value(), namespaceName,
                    SystemTopicNames.NAMESPACE_EVENTS_LOCAL_NAME);
            case TRANSACTION_BUFFER_SNAPSHOT -> TopicName.get(TopicDomain.persistent.value(), namespaceName,
                    SystemTopicNames.TRANSACTION_BUFFER_SNAPSHOT);
            case TRANSACTION_BUFFER_SNAPSHOT_SEGMENTS -> TopicName.get(TopicDomain.persistent.value(), namespaceName,
                    SystemTopicNames.TRANSACTION_BUFFER_SNAPSHOT_SEGMENTS);
            case TRANSACTION_BUFFER_SNAPSHOT_INDEXES -> TopicName.get(TopicDomain.persistent.value(), namespaceName,
                    SystemTopicNames.TRANSACTION_BUFFER_SNAPSHOT_INDEXES);
        };
    }

    private static final Logger log = LoggerFactory.getLogger(NamespaceEventsSystemTopicFactory.class);
}
