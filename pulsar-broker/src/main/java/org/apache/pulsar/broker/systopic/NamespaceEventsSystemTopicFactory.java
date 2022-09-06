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

import org.apache.pulsar.broker.service.TransactionBufferSnapshotIndexService;
import org.apache.pulsar.broker.service.TransactionBufferSnapshotSegmentService;
import org.apache.pulsar.broker.service.TransactionBufferSnapshotService;
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
        TopicName topicName = TopicName.get(TopicDomain.persistent.value(), namespaceName,
                SystemTopicNames.NAMESPACE_EVENTS_LOCAL_NAME);
        log.info("Create topic policies system topic client {}", topicName.toString());
        return new TopicPoliciesSystemTopicClient(client, topicName);
    }

    public TransactionBufferSystemTopicClient createTransactionBufferSystemTopicClient(NamespaceName namespaceName,
                                                   TransactionBufferSnapshotService transactionBufferSnapshotService) {
        TopicName topicName = TopicName.get(TopicDomain.persistent.value(), namespaceName,
                SystemTopicNames.TRANSACTION_BUFFER_SNAPSHOT);
        log.info("Create transaction buffer snapshot client, topicName : {}", topicName.toString());
        return new TransactionBufferSystemTopicClient(client, topicName, transactionBufferSnapshotService);
    }

    public TransactionBufferSnapshotIndexSystemTopicClient createTransactionBufferSnapshotIndexSystemTopicClient(
            NamespaceName namespaceName, TransactionBufferSnapshotIndexService transactionBufferSnapshotIndexService) {
        TopicName topicName = TopicName.get(TopicDomain.persistent.value(), namespaceName,
                SystemTopicNames.TRANSACTION_BUFFER_SNAPSHOT_INDEXES);
        log.info("Create transaction buffer snapshot index client, topicName : {}", topicName.toString());
        return new TransactionBufferSnapshotIndexSystemTopicClient(client, topicName,
                transactionBufferSnapshotIndexService);
    }

    public TransactionBufferSnapshotSegmentSystemTopicClient createTransactionBufferSnapshotSegmentSystemTopicClient(
            NamespaceName namespaceName,
            TransactionBufferSnapshotSegmentService transactionBufferSnapshotSegmentService) {
        TopicName topicName = TopicName.get(TopicDomain.persistent.value(), namespaceName,
                SystemTopicNames.TRANSACTION_BUFFER_SNAPSHOT_SEGMENT);
        log.info("Create transaction buffer snapshot segment client, topicName : {}", topicName.toString());
        return new TransactionBufferSnapshotSegmentSystemTopicClient(client, topicName,
                transactionBufferSnapshotSegmentService);
    }

    public static TopicName getSystemTopicName(NamespaceName namespaceName, EventType eventType) {
        return switch (eventType) {
            case TOPIC_POLICY -> TopicName.get(TopicDomain.persistent.value(), namespaceName,
                    SystemTopicNames.NAMESPACE_EVENTS_LOCAL_NAME);
            case TRANSACTION_BUFFER_SNAPSHOT -> TopicName.get(TopicDomain.persistent.value(), namespaceName,
                    SystemTopicNames.TRANSACTION_BUFFER_SNAPSHOT);
            case TRANSACTION_BUFFER_SNAPSHOT_SEGMENT -> TopicName.get(TopicDomain.persistent.value(), namespaceName,
                    SystemTopicNames.TRANSACTION_BUFFER_SNAPSHOT_SEGMENT);
            case TRANSACTION_BUFFER_SNAPSHOT_INDEXES -> TopicName.get(TopicDomain.persistent.value(), namespaceName,
                    SystemTopicNames.TRANSACTION_BUFFER_SNAPSHOT_INDEXES);
        };
    }

    private static final Logger log = LoggerFactory.getLogger(NamespaceEventsSystemTopicFactory.class);
}
