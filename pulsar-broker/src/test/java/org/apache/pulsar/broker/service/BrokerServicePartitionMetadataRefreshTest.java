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
package org.apache.pulsar.broker.service;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.broker.namespace.LookupOptions;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.Notification;
import org.apache.pulsar.metadata.impl.LocalMemoryMetadataStore;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class BrokerServicePartitionMetadataRefreshTest extends SharedPulsarBaseTest {

    @DataProvider
    public Object[][] topicDomains() {
        return new Object[][]{{"persistent"}, {"non-persistent"}};
    }

    @DataProvider
    public Object[][] stalePartitionCounts() {
        return new Object[][]{
                {"persistent", 1}, {"non-persistent", 1},
                {"persistent", 0}, {"non-persistent", 0}
        };
    }

    @Test(dataProvider = "stalePartitionCounts", timeOut = 30_000)
    public void testNewPartitionLoadsWithStaleMetadata(String domain, int cachedPartitions) throws Exception {
        TopicName topic = newTopicName(domain);
        try (DelayedNotificationsStore writer = metadataWriter(topic)) {
            prepareMetadata(topic, writer, cachedPartitions, 2);
            TopicName partition = topic.getPartition(1);
            assertThat(getTopicReference(partition.toString())).isEmpty();

            assertThat(getTopic(partition.toString(), true).get(10, SECONDS)).isPresent();
            assertCachedPartitions(topic, 2);
            try (Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                    .topic(partition.toString()).create()) {
                producer.send("new partition");
            }
        }
    }

    @Test(dataProvider = "topicDomains", timeOut = 30_000)
    public void testValidCachedPartitionDoesNotRefresh(String domain) throws Exception {
        TopicName topic = newTopicName(domain);
        try (DelayedNotificationsStore writer = metadataWriter(topic)) {
            prepareMetadata(topic, writer, 1, 2);
            TopicName partition = topic.getPartition(0);
            assertThat(getTopicReference(partition.toString())).isEmpty();

            assertThat(getTopic(partition.toString(), true).get(10, SECONDS)).isPresent();
            // The stored count is two. Keeping one in the cache proves that the valid load did not refresh it.
            assertCachedPartitions(topic, 1);
        }
    }

    @Test(dataProvider = "topicDomains", timeOut = 30_000)
    public void testInvalidPartitionIsRejectedUsingRefreshedCount(String domain) throws Exception {
        TopicName topic = newTopicName(domain);
        try (DelayedNotificationsStore writer = metadataWriter(topic)) {
            prepareMetadata(topic, writer, 1, 2);
            TopicName invalidPartition = topic.getPartition(2);

            assertThatThrownBy(() -> getTopic(invalidPartition.toString(), true).get(10, SECONDS))
                    .hasRootCauseInstanceOf(BrokerServiceException.NotAllowedException.class)
                    .hasRootCauseMessage("Illegal topic partition name " + invalidPartition
                            + " with max allowed 2 partitions");
            assertCachedPartitions(topic, 2);
            assertThat(getTopicReference(invalidPartition.toString())).isEmpty();
        }
    }

    @Test(dataProvider = "topicDomains", timeOut = 30_000)
    public void testMissingPartitionMetadataRemainsRejected(String domain) throws Exception {
        TopicName topic = newTopicName(domain);
        try (DelayedNotificationsStore writer = metadataWriter(topic)) {
            prepareMetadata(topic, writer, 0, 0);
            TopicName partition = topic.getPartition(0);

            assertThatThrownBy(() -> getTopic(partition.toString(), true).get(10, SECONDS))
                    .hasRootCauseInstanceOf(BrokerServiceException.NotAllowedException.class)
                    .hasRootCauseMessage("Partition metadata not found for the partitioned topic: " + partition);
            assertThat(getTopicReference(partition.toString())).isEmpty();
        }
    }

    @Test(dataProvider = "topicDomains", timeOut = 30_000)
    public void testPartitionedBaseNameRemainsRejectedWithoutRefresh(String domain) throws Exception {
        TopicName topic = newTopicName(domain);
        try (DelayedNotificationsStore writer = metadataWriter(topic)) {
            prepareMetadata(topic, writer, 1, 2);

            assertThatThrownBy(() -> getTopic(topic.toString(), true).get(10, SECONDS))
                    .hasRootCauseInstanceOf(BrokerServiceException.NotAllowedException.class)
                    .hasRootCauseMessage("Found partitioned metadata for non-partitioned topic: " + topic);
            assertCachedPartitions(topic, 1);
        }
    }

    private TopicName newTopicName(String domain) {
        TopicName name = TopicName.get(newTopicName());
        return TopicName.get(domain, name.getNamespaceObject(), name.getLocalName());
    }

    private DelayedNotificationsStore metadataWriter(TopicName topic) throws Exception {
        return new DelayedNotificationsStore(getConfig().getConfigurationMetadataStoreUrl(),
                "/admin/partitioned-topics/" + topic.getNamespace() + "/" + topic.getDomain().value()
                        + "/" + topic.getEncodedLocalName());
    }

    private void prepareMetadata(TopicName topic, DelayedNotificationsStore writer,
                                 int cachedPartitions, int storedPartitions) throws Exception {
        assertThat(getPulsar().getNamespaceService().getBrokerServiceUrlAsync(topic.getPartition(0),
                LookupOptions.builder().authoritative(true).build()).get(10, SECONDS)).isPresent();
        if (cachedPartitions > 0) {
            writer.writePartitions(cachedPartitions);
        }
        assertCachedPartitions(topic, cachedPartitions);
        if (storedPartitions > 0) {
            writer.writePartitions(storedPartitions);
        }
        assertCachedPartitions(topic, cachedPartitions);
    }

    private void assertCachedPartitions(TopicName topic, int expected) throws Exception {
        assertThat(getPulsar().getBrokerService().fetchPartitionedTopicMetadataAsync(topic)
                .get(5, SECONDS).partitions).isEqualTo(expected);
    }

    /** A real writer sharing the broker's data set, with only this topic's watch notifications delayed. */
    private static final class DelayedNotificationsStore extends LocalMemoryMetadataStore {
        private final String delayedPath;
        private final List<Notification> delayedNotifications = new ArrayList<>();

        private DelayedNotificationsStore(String url, String delayedPath) throws Exception {
            super(url, MetadataStoreConfig.builder().metadataStoreName("partition-metadata-writer").build());
            this.delayedPath = delayedPath;
        }

        private void writePartitions(int partitions) throws Exception {
            put(delayedPath, ObjectMapperFactory.getMapper().writer()
                    .writeValueAsBytes(new PartitionedTopicMetadata(partitions)), Optional.empty()).get(5, SECONDS);
        }

        @Override
        protected CompletableFuture<Void> receivedNotification(Notification notification) {
            if (notification.getPath().equals(delayedPath)) {
                delayedNotifications.add(notification);
                return CompletableFuture.completedFuture(null);
            }
            return super.receivedNotification(notification);
        }

        @Override
        public void close() throws Exception {
            try {
                FutureUtil.waitForAll(delayedNotifications.stream().map(super::receivedNotification).toList())
                        .get(5, SECONDS);
            } finally {
                super.close();
            }
        }
    }
}
