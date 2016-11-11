/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.pulsar.broker.service;

import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.lang.reflect.Field;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.testng.collections.Lists;

import com.yahoo.pulsar.broker.namespace.NamespaceService;
import com.yahoo.pulsar.broker.namespace.OwnedBundle;
import com.yahoo.pulsar.broker.namespace.OwnershipCache;
import com.yahoo.pulsar.broker.service.persistent.PersistentReplicator;
import com.yahoo.pulsar.broker.service.persistent.PersistentTopic;
import com.yahoo.pulsar.client.admin.PulsarAdminException;
import com.yahoo.pulsar.client.admin.PulsarAdminException.PreconditionFailedException;
import com.yahoo.pulsar.client.api.MessageBuilder;
import com.yahoo.pulsar.client.api.PulsarClient;
import com.yahoo.pulsar.common.naming.DestinationName;
import com.yahoo.pulsar.common.naming.NamespaceBundle;
import com.yahoo.pulsar.common.naming.NamespaceName;
import com.yahoo.pulsar.common.policies.data.ReplicatorStats;
import com.yahoo.pulsar.common.util.collections.ConcurrentOpenHashMap;

/**
 * Starts 2 brokers that are in 2 different clusters
 */
public class ReplicatorTest extends ReplicatorTestBase {

    @Override
    @BeforeClass
    void setup() throws Exception {
        super.setup();
    }

    @Override
    @AfterClass
    void shutdown() throws Exception {
        super.shutdown();
    }

    @Test(enabled = true)
    public void testConfigChange() throws Exception {
        log.info("--- Starting ReplicatorTest::testConfigChange ---");
        // This test is to verify that the config change on global namespace is successfully applied in broker during
        // runtime.
        // Run a set of producer tasks to create the destinations
        List<Future<Void>> results = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
            final DestinationName dest = DestinationName
                    .get(String.format("persistent://pulsar/global/ns/topic-%d", i));

            results.add(executor.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {

                    MessageProducer producer = new MessageProducer(url1, dest);
                    log.info("--- Starting producer --- " + url1);

                    MessageConsumer consumer = new MessageConsumer(url1, dest);
                    log.info("--- Starting Consumer --- " + url1);

                    producer.produce(2);

                    consumer.receive(2);

                    producer.close();
                    consumer.close();
                    return null;
                }
            }));
        }

        for (Future<Void> result : results) {
            try {
                result.get();
            } catch (Exception e) {
                log.error("exception in getting future result ", e);
                fail(String.format("replication test failed with %s exception", e.getMessage()));
            }
        }

        Thread.sleep(1000L);
        // Make sure that the internal replicators map contains remote cluster info
        ConcurrentOpenHashMap<String, PulsarClient> replicationClients1 = ns1.getReplicationClients();
        ConcurrentOpenHashMap<String, PulsarClient> replicationClients2 = ns2.getReplicationClients();
        ConcurrentOpenHashMap<String, PulsarClient> replicationClients3 = ns3.getReplicationClients();

        Assert.assertNotNull(replicationClients1.get("r2"));
        Assert.assertNotNull(replicationClients1.get("r3"));
        Assert.assertNotNull(replicationClients2.get("r1"));
        Assert.assertNotNull(replicationClients2.get("r3"));
        Assert.assertNotNull(replicationClients3.get("r1"));
        Assert.assertNotNull(replicationClients3.get("r2"));

        // Case 1: Update the global namespace replication configuration to only contains the local cluster itself
        admin1.namespaces().setNamespaceReplicationClusters("pulsar/global/ns", Lists.newArrayList("r1"));

        // Wait for config changes to be updated.
        Thread.sleep(1000L);

        // Make sure that the internal replicators map still contains remote cluster info
        Assert.assertNotNull(replicationClients1.get("r2"));
        Assert.assertNotNull(replicationClients1.get("r3"));
        Assert.assertNotNull(replicationClients2.get("r1"));
        Assert.assertNotNull(replicationClients2.get("r3"));
        Assert.assertNotNull(replicationClients3.get("r1"));
        Assert.assertNotNull(replicationClients3.get("r2"));

        // Case 2: Update the configuration back
        admin1.namespaces().setNamespaceReplicationClusters("pulsar/global/ns", Lists.newArrayList("r1", "r2", "r3"));

        // Wait for config changes to be updated.
        Thread.sleep(1000L);

        // Make sure that the internal replicators map still contains remote cluster info
        Assert.assertNotNull(replicationClients1.get("r2"));
        Assert.assertNotNull(replicationClients1.get("r3"));
        Assert.assertNotNull(replicationClients2.get("r1"));
        Assert.assertNotNull(replicationClients2.get("r3"));
        Assert.assertNotNull(replicationClients3.get("r1"));
        Assert.assertNotNull(replicationClients3.get("r2"));

        // Case 3: TODO: Once automatic cleanup is implemented, add tests case to verify auto removal of clusters
    }

    @Test(enabled = false)
    public void testConfigChangeNegativeCases() throws Exception {
        log.info("--- Starting ReplicatorTest::testConfigChangeNegativeCases ---");
        // Negative test cases for global namespace config change. Verify that the namespace config change can not be
        // updated when the namespace is being unloaded.
        // Set up field access to internal namespace state in NamespaceService
        Field ownershipCacheField = NamespaceService.class.getDeclaredField("ownershipCache");
        ownershipCacheField.setAccessible(true);
        OwnershipCache ownerCache = (OwnershipCache) ownershipCacheField.get(pulsar1.getNamespaceService());
        Assert.assertNotNull(pulsar1, "pulsar1 is null");
        Assert.assertNotNull(pulsar1.getNamespaceService(), "pulsar1.getNamespaceService() is null");
        NamespaceBundle globalNsBundle = pulsar1.getNamespaceService().getNamespaceBundleFactory()
                .getFullBundle(new NamespaceName("pulsar/global/ns"));
        ownerCache.tryAcquiringOwnership(globalNsBundle);
        Assert.assertNotNull(ownerCache.getOwnedBundle(globalNsBundle),
                "pulsar1.getNamespaceService().getOwnedServiceUnit(new NamespaceName(\"pulsar/global/ns\")) is null");
        Field stateField = OwnedBundle.class.getDeclaredField("isActive");
        stateField.setAccessible(true);
        // set the namespace to be disabled
        ownerCache.disableOwnership(globalNsBundle);

        // Make sure the namespace config update failed
        try {
            admin1.namespaces().setNamespaceReplicationClusters("pulsar/global/ns", Lists.newArrayList("r1"));
            fail("Should have raised exception");
        } catch (PreconditionFailedException pfe) {
            // OK
        }

        // restore the namespace state
        ownerCache.removeOwnership(globalNsBundle);
        ownerCache.tryAcquiringOwnership(globalNsBundle);
    }

    @Test(enabled = true)
    public void testReplication() throws Exception {

        log.info("--- Starting ReplicatorTest::testReplication ---");

        // This test is to verify that the config change on global namespace is successfully applied in broker during
        // runtime.
        // Run a set of producer tasks to create the destinations
        SortedSet<String> testDests = new TreeSet<String>();
        List<Future<Void>> results = Lists.newArrayList();
        for (int i = 0; i < 3; i++) {
            final DestinationName dest = DestinationName
                    .get(String.format("persistent://pulsar/global/ns/repltopic-%d", i));
            testDests.add(dest.toString());

            results.add(executor.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {

                    MessageProducer producer1 = new MessageProducer(url1, dest);
                    log.info("--- Starting producer --- " + url1);

                    MessageProducer producer2 = new MessageProducer(url2, dest);
                    log.info("--- Starting producer --- " + url2);

                    MessageProducer producer3 = new MessageProducer(url3, dest);
                    log.info("--- Starting producer --- " + url3);

                    MessageConsumer consumer1 = new MessageConsumer(url1, dest);
                    log.info("--- Starting Consumer --- " + url1);

                    MessageConsumer consumer2 = new MessageConsumer(url2, dest);
                    log.info("--- Starting Consumer --- " + url2);

                    MessageConsumer consumer3 = new MessageConsumer(url3, dest);
                    log.info("--- Starting Consumer --- " + url3);

                    // Produce from cluster1 and consume from the rest
                    producer1.produce(2);

                    consumer1.receive(2);

                    consumer2.receive(2);

                    consumer3.receive(2);

                    // Produce from cluster2 and consume from the rest
                    producer2.produce(2);

                    consumer1.receive(2);

                    consumer2.receive(2);

                    consumer3.receive(2);

                    // Produce from cluster3 and consume from the rest
                    producer3.produce(2);

                    consumer1.receive(2);

                    consumer2.receive(2);

                    consumer3.receive(2);

                    // Produce from cluster1&2 and consume from cluster3
                    producer1.produce(1);
                    producer2.produce(1);

                    consumer1.receive(1);

                    consumer2.receive(1);

                    consumer3.receive(1);

                    consumer1.receive(1);

                    consumer2.receive(1);

                    consumer3.receive(1);

                    producer1.close();
                    producer2.close();
                    producer3.close();
                    consumer1.close();
                    consumer2.close();
                    consumer3.close();
                    return null;
                }
            }));
        }

        for (Future<Void> result : results) {
            try {
                result.get();
            } catch (Exception e) {
                log.error("exception in getting future result ", e);
                fail(String.format("replication test failed with %s exception", e.getMessage()));
            }
        }
    }

    @Test(enabled = false)
    public void testReplicationOverrides() throws Exception {

        log.info("--- Starting ReplicatorTest::testReplication ---");

        // This test is to verify that the config change on global namespace is successfully applied in broker during
        // runtime.
        // Run a set of producer tasks to create the destinations
        SortedSet<String> testDests = new TreeSet<String>();
        List<Future<Void>> results = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
            final DestinationName dest = DestinationName
                    .get(String.format("persistent://pulsar/global/ns/repltopic-%d", i));
            testDests.add(dest.toString());

            results.add(executor.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {

                    MessageProducer producer1 = new MessageProducer(url1, dest);
                    log.info("--- Starting producer --- " + url1);

                    MessageProducer producer2 = new MessageProducer(url2, dest);
                    log.info("--- Starting producer --- " + url2);

                    MessageProducer producer3 = new MessageProducer(url3, dest);
                    log.info("--- Starting producer --- " + url3);

                    MessageConsumer consumer1 = new MessageConsumer(url1, dest);
                    log.info("--- Starting Consumer --- " + url1);

                    MessageConsumer consumer2 = new MessageConsumer(url2, dest);
                    log.info("--- Starting Consumer --- " + url2);

                    MessageConsumer consumer3 = new MessageConsumer(url3, dest);
                    log.info("--- Starting Consumer --- " + url3);

                    // Produce from cluster1 for this test
                    int nr1 = 0;
                    int nr2 = 0;
                    int nR3 = 0;

                    // Produce a message that isn't replicated
                    producer1.produce(1, MessageBuilder.create().disableReplication());

                    // Produce a message not replicated to r2
                    producer1.produce(1,
                            MessageBuilder.create().setReplicationClusters(Lists.newArrayList("r1", "r3")));

                    // Produce a default replicated message
                    producer1.produce(1);

                    consumer1.receive(3);
                    consumer2.receive(1);
                    if (!consumer2.drained()) {
                        throw new Exception("consumer2 - unexpected message in queue");
                    }
                    consumer3.receive(2);
                    if (!consumer3.drained()) {
                        throw new Exception("consumer3 - unexpected message in queue");
                    }

                    producer1.close();
                    producer2.close();
                    producer3.close();
                    consumer1.close();
                    consumer2.close();
                    consumer3.close();
                    return null;
                }
            }));
        }

        for (Future<Void> result : results) {
            try {
                result.get();
            } catch (Exception e) {
                log.error("exception in getting future result ", e);
                fail(String.format("replication test failed with %s exception", e.getMessage()));
            }
        }
    }

    @Test(enabled = true)
    public void testFailures() throws Exception {

        log.info("--- Starting ReplicatorTest::testFailures ---");

        try {
            // 1. Create a consumer using the reserved consumer id prefix "pulsar.repl."

            final DestinationName dest = DestinationName
                    .get(String.format("persistent://pulsar/global/ns/res-cons-id"));

            // Create another consumer using replication prefix as sub id
            MessageConsumer consumer = new MessageConsumer(url2, dest, "pulsar.repl.");
            consumer.close();

        } catch (Exception e) {
            // SUCCESS
        }

    }

    @Test
    public void testReplicatePeekAndSkip() throws Exception {

        SortedSet<String> testDests = new TreeSet<String>();

        final DestinationName dest = DestinationName.get("persistent://pulsar/global/ns/peekAndSeekTopic");
        testDests.add(dest.toString());

        MessageProducer producer1 = new MessageProducer(url1, dest);
        MessageConsumer consumer1 = new MessageConsumer(url3, dest);

        // Produce from cluster1 and consume from the rest
        producer1.produce(2);
        producer1.close();
        PersistentTopic topic = (PersistentTopic) pulsar1.getBrokerService().getTopicReference(dest.toString());
        PersistentReplicator replicator = topic.getReplicators().get(topic.getReplicators().keys().get(0));
        replicator.skipMessages(2);
        CompletableFuture<Entry> result = replicator.peekNthMessage(1);
        Entry entry = result.get(50, TimeUnit.MILLISECONDS);
        assertNull(entry);
        consumer1.close();
    }

    @Test
    public void testReplicatorClearBacklog() throws Exception {

        // This test is to verify that reset cursor fails on global topic
        SortedSet<String> testDests = new TreeSet<String>();

        final DestinationName dest = DestinationName.get("persistent://pulsar/global/ns/clearBacklogTopic");
        testDests.add(dest.toString());

        MessageProducer producer1 = new MessageProducer(url1, dest);
        MessageConsumer consumer1 = new MessageConsumer(url3, dest);

        // Produce from cluster1 and consume from the rest
        producer1.produce(2);
        producer1.close();
        PersistentTopic topic = (PersistentTopic) pulsar1.getBrokerService().getTopicReference(dest.toString());
        PersistentReplicator replicator = spy(topic.getReplicators().get(topic.getReplicators().keys().get(0)));
        replicator.readEntriesFailed(new ManagedLedgerException.InvalidCursorPositionException("failed"), null);
        replicator.clearBacklog().get();
        Thread.sleep(100);
        replicator.updateRates(); // for code-coverage
        replicator.expireMessages(1); // for code-coverage
        ReplicatorStats status = replicator.getStats();
        assertTrue(status.replicationBacklog == 0);
        consumer1.close();
    }

    @Test(enabled = true)
    public void testResetCursorNotFail() throws Exception {

        log.info("--- Starting ReplicatorTest::testResetCursorNotFail ---");

        // This test is to verify that reset cursor fails on global topic
        SortedSet<String> testDests = new TreeSet<String>();
        List<Future<Void>> results = Lists.newArrayList();
        for (int i = 0; i < 1; i++) {
            final DestinationName dest = DestinationName
                    .get(String.format("persistent://pulsar/global/ns/resetrepltopic-%d", i));
            testDests.add(dest.toString());

            results.add(executor.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {

                    MessageProducer producer1 = new MessageProducer(url1, dest);
                    log.info("--- Starting producer --- " + url1);

                    MessageConsumer consumer1 = new MessageConsumer(url1, dest);
                    log.info("--- Starting Consumer --- " + url1);

                    // Produce from cluster1 and consume from the rest
                    producer1.produce(2);

                    consumer1.receive(2);

                    producer1.close();
                    consumer1.close();
                    return null;
                }
            }));
        }

        for (Future<Void> result : results) {
            try {
                result.get();
            } catch (Exception e) {
                log.error("exception in getting future result ", e);
                fail(String.format("replication test failed with %s exception", e.getMessage()));
            }
        }
        admin1.persistentTopics().resetCursor(testDests.first(), "sub-id", System.currentTimeMillis());
    }

    @Test(enabled = true)
    public void testReplicationForBatchMessages() throws Exception {

        log.info("--- Starting ReplicatorTest::testReplicationForBatchMessages ---");

        // Run a set of producer tasks to create the destinations
        SortedSet<String> testDests = new TreeSet<String>();
        List<Future<Void>> results = Lists.newArrayList();
        for (int i = 0; i < 3; i++) {
            final DestinationName dest = DestinationName
                    .get(String.format("persistent://pulsar/global/ns/repltopicbatch-%d", i));
            testDests.add(dest.toString());

            results.add(executor.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {

                    MessageProducer producer1 = new MessageProducer(url1, dest, true);
                    log.info("--- Starting producer --- " + url1);

                    MessageProducer producer2 = new MessageProducer(url2, dest, true);
                    log.info("--- Starting producer --- " + url2);

                    MessageProducer producer3 = new MessageProducer(url3, dest, true);
                    log.info("--- Starting producer --- " + url3);

                    MessageConsumer consumer1 = new MessageConsumer(url1, dest);
                    log.info("--- Starting Consumer --- " + url1);

                    MessageConsumer consumer2 = new MessageConsumer(url2, dest);
                    log.info("--- Starting Consumer --- " + url2);

                    MessageConsumer consumer3 = new MessageConsumer(url3, dest);
                    log.info("--- Starting Consumer --- " + url3);

                    // Produce from cluster1 and consume from the rest
                    producer1.produceBatch(10);

                    consumer1.receive(10);

                    consumer2.receive(10);

                    consumer3.receive(10);

                    // Produce from cluster2 and consume from the rest
                    producer2.produceBatch(10);

                    consumer1.receive(10);

                    consumer2.receive(10);

                    consumer3.receive(10);

                    return null;
                }
            }));
        }

        for (Future<Void> result : results) {
            try {
                result.get(5, TimeUnit.SECONDS);
            } catch (Exception e) {
                log.error("exception in getting future result ", e);
                fail(String.format("replication test failed with %s exception", e.getMessage()));
            }
        }
    }

    private static final Logger log = LoggerFactory.getLogger(ReplicatorTest.class);

}
