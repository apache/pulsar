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
package org.apache.pulsar.broker.service.persistent;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.TopicFactory;

/**
 * A {@link PersistentTopic} that retries a failed policy update after 1 second instead of 60, and records what the
 * retries do once the topic is closed.
 */
public class ShortPolicyRetryTopic extends PersistentTopic {

    private final AtomicInteger failedReplicationChecks = new AtomicInteger();
    private final AtomicInteger retriesAfterClose = new AtomicInteger();
    private final AtomicInteger replicationChecksAfterClose = new AtomicInteger();

    public ShortPolicyRetryTopic(String topic, ManagedLedger ledger, BrokerService brokerService) {
        super(topic, ledger, brokerService);
    }

    @Override
    long getPolicyUpdateFailureRetryDelaySeconds() {
        return 1;
    }

    @Override
    CompletableFuture<Void> checkReplicationAndRetryOnFailure() {
        boolean closed = isClosed();
        CompletableFuture<Void> result = super.checkReplicationAndRetryOnFailure();
        if (closed) {
            retriesAfterClose.incrementAndGet();
        }
        result.exceptionally(ex -> {
            failedReplicationChecks.incrementAndGet();
            return null;
        });
        return result;
    }

    @Override
    public CompletableFuture<Void> checkReplication() {
        if (isClosed()) {
            replicationChecksAfterClose.incrementAndGet();
        }
        return super.checkReplication();
    }

    /**
     * Returns how many policy-update replication checks failed. Each one has scheduled its retry by the time it is
     * counted.
     */
    public int getFailedReplicationChecks() {
        return failedReplicationChecks.get();
    }

    /**
     * Returns how many policy-update replication checks started after the topic was closed. Only a retry scheduled
     * before the close can start one.
     */
    public int getRetriesAfterClose() {
        return retriesAfterClose.get();
    }

    /**
     * Returns how many replication checks started after the topic was closed.
     */
    public int getReplicationChecksAfterClose() {
        return replicationChecksAfterClose.get();
    }

    /**
     * Creates a {@link ShortPolicyRetryTopic} for the topics registered with {@link #register(String)}. For any other
     * topic, persistent or not, it returns {@code null}, so the broker creates its default topic.
     */
    public static class Factory implements TopicFactory {

        private static final Set<String> TOPICS = ConcurrentHashMap.newKeySet();

        public static void register(String topic) {
            TOPICS.add(topic);
        }

        public static void unregister(String topic) {
            TOPICS.remove(topic);
        }

        @Override
        public <T extends Topic> T create(String topic, ManagedLedger ledger, BrokerService brokerService,
                                          Class<T> topicClazz) {
            if (topicClazz != PersistentTopic.class || !TOPICS.contains(topic)) {
                return null;
            }
            return topicClazz.cast(new ShortPolicyRetryTopic(topic, ledger, brokerService));
        }
    }
}
