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
package org.apache.pulsar.broker.stats;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.mledger.LedgerOffloader;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.impl.LedgerOffloaderStatsImpl;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.BrokerTestBase;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.junit.Assert;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

public class LedgerOffloaderMetricsTest  extends BrokerTestBase {

    @Override
    protected void setup() throws Exception {
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testTopicLevelMetrics() throws Exception {
        conf.setExposeTopicLevelMetricsInPrometheus(true);
        super.baseSetup();

        String ns1 = "prop/ns-abc1";
        admin.namespaces().createNamespace(ns1);
        String []topics = new String[3];

        LedgerOffloaderStatsImpl offloaderStats = (LedgerOffloaderStatsImpl) pulsar.getOffloaderStats();

        LedgerOffloader offloader = Mockito.mock(LedgerOffloader.class);
        Topic topic = Mockito.mock(PersistentTopic.class);
        CompletableFuture<Optional<Topic>> topicFuture = new CompletableFuture<>();
        Optional<Topic> topicOptional = Optional.of(topic);
        topicFuture.complete(topicOptional);
        BrokerService brokerService = spy(pulsar.getBrokerService());
        doReturn(brokerService).when(pulsar).getBrokerService();


        for (int i = 0; i < 3; i++) {
            String topicName = "persistent://prop/ns-abc1/testMetrics" + UUID.randomUUID();
            topics[i] = topicName;
            admin.topics().createNonPartitionedTopic(topicName);

            doReturn(topicFuture).when(brokerService).getTopicIfExists(topicName);
            Assert.assertTrue(topic instanceof PersistentTopic);

            ManagedLedger ledgerM = Mockito.mock(ManagedLedger.class);
            doReturn(ledgerM).when(((PersistentTopic) topic)).getManagedLedger();
            ManagedLedgerConfig config = Mockito.mock(ManagedLedgerConfig.class);
            doReturn(config).when(ledgerM).getConfig();
            doReturn(offloader).when(config).getLedgerOffloader();

            offloaderStats.recordOffloadError(topicName);
            offloaderStats.recordOffloadError(topicName);
            offloaderStats.recordOffloadBytes(topicName, 100);
            offloaderStats.recordReadLedgerLatency(topicName, 1000, TimeUnit.NANOSECONDS);
            offloaderStats.recordReadOffloadError(topicName);
            offloaderStats.recordReadOffloadError(topicName);
            offloaderStats.recordReadOffloadIndexLatency(topicName, 1000000L, TimeUnit.NANOSECONDS);
            offloaderStats.recordReadOffloadBytes(topicName, 100000);
            offloaderStats.recordWriteToStorageError(topicName);
            offloaderStats.recordWriteToStorageError(topicName);
        }

        for (String topicName : topics) {
            Assert.assertEquals(offloaderStats.getOffloadError(topicName), 2);
            Assert.assertEquals(offloaderStats.getOffloadBytes(topicName) , 100);
            Assert.assertEquals((long) offloaderStats.getReadLedgerLatency(topicName).sum, 1);
            Assert.assertEquals(offloaderStats.getReadOffloadError(topicName), 2);
            Assert.assertEquals((long) offloaderStats.getReadOffloadIndexLatency(topicName).sum ,1000);
            Assert.assertEquals(offloaderStats.getReadOffloadBytes(topicName), 100000);
            Assert.assertEquals(offloaderStats.getWriteStorageError(topicName), 2);
        }
    }

    @Test
    public void testNamespaceLevelMetrics() throws Exception {
        conf.setExposeTopicLevelMetricsInPrometheus(false);
        super.baseSetup();

        String ns1 = "prop/ns-abc1";
        String ns2 = "prop/ns-abc2";

        LedgerOffloaderStatsImpl offloaderStats = (LedgerOffloaderStatsImpl) pulsar.getOffloaderStats();

        LedgerOffloader offloader = Mockito.mock(LedgerOffloader.class);
        Topic topic = Mockito.mock(PersistentTopic.class);
        CompletableFuture<Optional<Topic>> topicFuture = new CompletableFuture<>();
        Optional<Topic> topicOptional = Optional.of(topic);
        topicFuture.complete(topicOptional);
        BrokerService brokerService = spy(pulsar.getBrokerService());
        doReturn(brokerService).when(pulsar).getBrokerService();
        Queue<String> queue = new LinkedList<>();
        Map<String, List<String>> namespace2Topics = new HashMap<>();
        for (int s = 0; s < 2; s++) {
            String nameSpace = ns1;
            if (s == 1) {
                nameSpace = ns2;
            }
            namespace2Topics.put(nameSpace, new ArrayList<>());

            admin.namespaces().createNamespace(nameSpace);
            String baseTopic1 = "persistent://" + nameSpace + "/testMetrics";
            for (int i = 0; i < 6; i++) {
                String topicName = baseTopic1 + UUID.randomUUID();
                List<String> topicList = namespace2Topics.get(nameSpace);
                topicList.add(topicName);

                queue.add(topicName);
                admin.topics().createNonPartitionedTopic(topicName);
                doReturn(topicFuture).when(brokerService).getTopicIfExists(topicName);
                Assert.assertTrue(topic instanceof PersistentTopic);


                ManagedLedger ledgerM = Mockito.mock(ManagedLedger.class);
                doReturn(ledgerM).when(((PersistentTopic) topic)).getManagedLedger();
                ManagedLedgerConfig config = Mockito.mock(ManagedLedgerConfig.class);
                doReturn(config).when(ledgerM).getConfig();
                doReturn(offloader).when(config).getLedgerOffloader();
                Mockito.when(ledgerM.getName()).thenAnswer((Answer<String>) invocation -> queue.poll());

                offloaderStats.recordOffloadError(topicName);
                offloaderStats.recordOffloadBytes(topicName, 100);
                offloaderStats.recordReadLedgerLatency(topicName, 1000, TimeUnit.NANOSECONDS);
                offloaderStats.recordReadOffloadError(topicName);
                offloaderStats.recordReadOffloadIndexLatency(topicName, 1000000L, TimeUnit.NANOSECONDS);
                offloaderStats.recordReadOffloadBytes(topicName, 100000);
                offloaderStats.recordWriteToStorageError(topicName);
            }
        }

        for (Map.Entry<String, List<String>> entry : namespace2Topics.entrySet()) {
            String namespace = entry.getKey();
            List<String> topics = entry.getValue();
            String topicName = topics.get(0);

            Assert.assertTrue(offloaderStats.getOffloadError(topicName) >= 1);
            Assert.assertTrue(offloaderStats.getOffloadBytes(topicName) >= 100);
            Assert.assertTrue((long) offloaderStats.getReadLedgerLatency(topicName).sum >= 1);
            Assert.assertTrue(offloaderStats.getReadOffloadError(topicName) >= 1);
            Assert.assertTrue((long) offloaderStats.getReadOffloadIndexLatency(topicName).sum >= 1000);
            Assert.assertTrue(offloaderStats.getReadOffloadBytes(topicName) >= 100000);
            Assert.assertTrue(offloaderStats.getWriteStorageError(topicName) >= 1);
        }
    }

}
