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
package org.apache.pulsar.tests.integration.admin;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Supplier;

import lombok.Cleanup;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.tests.integration.messaging.MessagingBase;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

/**
 * Integration tests for Pulsar Admin.
 */
@Slf4j
public class AdminTest extends MessagingBase {

    @Test(dataProvider = "ServiceAndAdminUrls")
    public void testUnderReplicatedState(Supplier<String> serviceUrl, Supplier<String> adminUrl) throws Exception {

        String topicName = getNonPartitionedTopic("replicated-state", true);

        @Cleanup
        PulsarAdmin admin = PulsarAdmin.builder()
                .serviceHttpUrl(adminUrl.get())
                .build();

        @Cleanup final PulsarClient client = PulsarClient.builder()
                .serviceUrl(serviceUrl.get())
                .build();

        @Cleanup final Producer<String> producer = client.newProducer(Schema.STRING)
                .topic(topicName)
                .enableBatching(false)
                .create();

        for (int i = 0; i < 10; i++) {
            MessageId messageId = producer.newMessage().value(producer.getProducerName() + "-" + i).send();
            assertNotNull(messageId);
        }

        log.info("Successfully to publish 10 messages to {}", topicName);
        PersistentTopicInternalStats stats = admin.topics().getInternalStats(topicName);
        Assert.assertTrue(stats.ledgers.size() > 0);
        for (PersistentTopicInternalStats.LedgerInfo ledger : stats.ledgers) {
            Assert.assertFalse(ledger.underReplicated);
        }
    }


    @SneakyThrows
    @Test
    public void partitionedAndNonPartitionedConcurrentCreate() {
        final String nsName = "public/" + generateNamespaceName();
        pulsarAdmin.namespaces().createNamespace(nsName);
        final String topicPrefix = "persistent://" + nsName + "/";
        for (int i = 0; i < 300; i++) {
            final String tmpTopicName = topicPrefix + UUID.randomUUID();
            final List<CompletableFuture<Void>> futures = new ArrayList<>(2);
            final CompletableFuture<Void> partitionedFuture = pulsarAdmin.topics().createPartitionedTopicAsync(tmpTopicName, 1);
            final CompletableFuture<Void> nonPartitionedFuture = pulsarAdmin.topics().createNonPartitionedTopicAsync(tmpTopicName);
            futures.add(partitionedFuture);
            futures.add(nonPartitionedFuture);
            try {
                FutureUtil.waitForAll(futures).join();
                final List<String> topics = pulsarAdmin.topics().getList(nsName);
                Assert.assertTrue(topics.contains(tmpTopicName));
                Assert.assertTrue(topics.contains(TopicName.get(tmpTopicName).getPartition(0).toString()));
                fail("Expect admin conflict exception.");
            } catch (CompletionException ex) {
                Assert.assertTrue(ex.getCause() instanceof PulsarAdminException.ConflictException);
            }
            final List<String> topics = pulsarAdmin.topics().getList(nsName);
            assertTrue(topics.contains(tmpTopicName)
                    ^ topics.contains(TopicName.get(tmpTopicName).getPartition(0).toString()));
        }
    }

}
