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
package org.apache.pulsar.tests.integration.offload;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.apache.pulsar.tests.integration.suites.PulsarTieredStorageTestSuite;
import org.awaitility.Awaitility;
import org.testng.Assert;

@Slf4j
public abstract class TestBaseOffload extends PulsarTieredStorageTestSuite {
    protected int getEntrySize() {
        return 1024;
    };

    private byte[] buildEntry(String pattern) {
        byte[] entry = new byte[getEntrySize()];
        byte[] patternBytes = pattern.getBytes();

        for (int i = 0; i < entry.length; i++) {
            entry[i] = patternBytes[i % patternBytes.length];
        }
        return entry;
    }

    protected void testPublishOffloadAndConsumeViaCLI(String serviceUrl, String adminUrl) throws Exception {
        final String tenant = "offload-test-cli-" + randomName(4);
        final String namespace = tenant + "/ns1";
        final String topic = "persistent://" + namespace + "/topic1";

        pulsarCluster.runAdminCommandOnAnyBroker( "tenants",
                "create", "--allowed-clusters", pulsarCluster.getClusterName(),
                "--admin-roles", "offload-admin", tenant);

        pulsarCluster.runAdminCommandOnAnyBroker(
                "namespaces",
                "create", "--clusters", pulsarCluster.getClusterName(), namespace);

        long firstLedger = -1;
        try(PulsarClient client = PulsarClient.builder().serviceUrl(serviceUrl).build();
            Producer<byte[]> producer = client.newProducer().topic(topic)
                    .maxPendingMessages(getNumEntriesPerLedger() / 2).sendTimeout(60, TimeUnit.SECONDS)
                    .blockIfQueueFull(true).enableBatching(false).create();) {
            client.newConsumer().topic(topic).subscriptionName("my-sub").subscribe().close();

            // write enough to topic to make it roll
            int i = 0;
            AtomicBoolean success = new AtomicBoolean(true);

            for (; i < getNumEntriesPerLedger() * 1.5; i++) {
                producer.sendAsync(buildEntry("offload-message" + i))
                        .exceptionally(e -> {
                            log.error("failed to send a message", e);
                            success.set(false);
                            return null;
                        });;
            }
            producer.flush();
            Assert.assertTrue(success.get());
        }

        try (PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(adminUrl).build()) {
            // read managed ledger info, check ledgers exist
            firstLedger = admin.topics().getInternalStats(topic).ledgers.get(0).ledgerId;

            // first offload with a high threshold, nothing should offload

            String output = pulsarCluster.runAdminCommandOnAnyBroker("topics",
                    "offload", "--size-threshold", "100G", topic).getStdout();
            Assert.assertTrue(output.contains("which keep 107374182400 bytes on bookkeeper"));

            output = pulsarCluster.runAdminCommandOnAnyBroker( "topics",
                    "offload-status", topic).getStdout();
            Assert.assertTrue(output.contains("Offload has not been run"));

            // offload with a low threshold
            output = pulsarCluster.runAdminCommandOnAnyBroker("topics",
                    "offload", "--size-threshold", "1M", topic).getStdout();
            Assert.assertTrue(output.contains("Offload triggered"));

            output = pulsarCluster.runAdminCommandOnAnyBroker("topics",
                    "offload-status", "-w", topic).getStdout();
            Assert.assertTrue(output.contains("Offload was a success"));

            // delete the first ledger, so that we cannot possibly read from it
            ClientConfiguration bkConf = new ClientConfiguration();
            bkConf.setZkServers(pulsarCluster.getZKConnString());
            try (BookKeeper bk = new BookKeeper(bkConf)) {
                bk.deleteLedger(firstLedger);
            }

            // Unload topic to clear all caches, open handles, etc
            admin.topics().unload(topic);
        }

        log.info("Read back the data (which would be in that first ledger)");
        try(PulsarClient client = PulsarClient.builder().serviceUrl(serviceUrl).build();
            Consumer<byte[]> consumer = client.newConsumer().topic(topic).subscriptionName("my-sub").subscribe()) {
            // read back from topic
            for (int i = 0; i < getNumEntriesPerLedger() * 1.5; i++) {
                Message<byte[]> m = consumer.receive(1, TimeUnit.MINUTES);
                Assert.assertEquals(buildEntry("offload-message" + i), m.getData());
            }
        }
    }

    protected void testPublishOffloadAndConsumeViaThreshold(String serviceUrl, String adminUrl) throws Exception {
        final String tenant = "offload-test-threshold-" + randomName(4);
        final String namespace = tenant + "/ns1";
        final String topic = "persistent://" + namespace + "/topic1";

        pulsarCluster.runAdminCommandOnAnyBroker("tenants",
                "create", "--allowed-clusters", pulsarCluster.getClusterName(),
                "--admin-roles", "offload-admin", tenant);

        pulsarCluster.runAdminCommandOnAnyBroker("namespaces",
                "create", "--clusters", pulsarCluster.getClusterName(), namespace);

        pulsarCluster.runAdminCommandOnAnyBroker("namespaces",
                "set-offload-threshold", "--size", "1M", namespace);

        long firstLedger = 0;
        try(PulsarClient client = PulsarClient.builder().serviceUrl(serviceUrl).build();
            Producer<byte[]> producer = client.newProducer().topic(topic)
                    .maxPendingMessages(getNumEntriesPerLedger() / 2).sendTimeout(60, TimeUnit.SECONDS)
                    .blockIfQueueFull(true).enableBatching(false).create()) {

            client.newConsumer().topic(topic).subscriptionName("my-sub").subscribe().close();

            AtomicBoolean success = new AtomicBoolean(true);
            // write enough to topic to make it roll twice
            for (int i = 0; i < getNumEntriesPerLedger() * 2.5; i++) {
                producer.sendAsync(buildEntry("offload-message" + i))
                        .exceptionally(e -> {
                            log.error("failed to send a message", e);
                            success.set(false);
                            return null;
                        });;
            }

            producer.flush();
            Assert.assertTrue(success.get());
        }

        try (PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(adminUrl).build()) {
            firstLedger = admin.topics().getInternalStats(topic).ledgers.get(0).ledgerId;

            // wait up to 30 seconds for offload to occur
            for (int i = 0; i < 100 && !admin.topics().getInternalStats(topic).ledgers.get(0).offloaded; i++) {
                Thread.sleep(300);
            }
            Assert.assertTrue(admin.topics().getInternalStats(topic).ledgers.get(0).offloaded);

            // delete the first ledger, so that we cannot possibly read from it
            ClientConfiguration bkConf = new ClientConfiguration();
            bkConf.setZkServers(pulsarCluster.getZKConnString());
            try (BookKeeper bk = new BookKeeper(bkConf)) {
                bk.deleteLedger(firstLedger);
            }

            // Unload topic to clear all caches, open handles, etc
            admin.topics().unload(topic);
        }

        log.info("Read back the data (which would be in that first ledger)");
        try (PulsarClient client = PulsarClient.builder().serviceUrl(serviceUrl).build();
             Consumer<byte[]> consumer = client.newConsumer().topic(topic).subscriptionName("my-sub").subscribe()) {
            // read back from topic
            for (int i = 0; i < getNumEntriesPerLedger() * 2.5; i++) {
                Message<byte[]> m = consumer.receive(1, TimeUnit.MINUTES);
                Assert.assertNotNull(m);
                Assert.assertEquals(buildEntry("offload-message" + i), m.getData());
            }
        }

        // try disabling
        pulsarCluster.runAdminCommandOnAnyBroker("namespaces",
                "set-offload-threshold", "--size", "-1", namespace);

        // hard to validate that it has been disabled as we'd be waiting for
        // something _not_ to happen (i.e. waiting for ages), so just check
        try (PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(adminUrl).build()) {
            Assert.assertEquals(admin.namespaces().getOffloadThreshold(namespace), -1L);
        }
    }

    private boolean ledgerOffloaded(List<PersistentTopicInternalStats.LedgerInfo> ledgers, long ledgerId) {
        return ledgers.stream().filter(l -> l.ledgerId == ledgerId)
                .map(l -> l.offloaded).findFirst().get();
    }

    private long writeAndWaitForOffload(String serviceUrl, String adminUrl, String topic)
            throws Exception {
        return writeAndWaitForOffload(serviceUrl, adminUrl, topic, -1);
    }

    private long writeAndWaitForOffload(String serviceUrl, String adminUrl, String topic, int partitionNum)
            throws Exception {
        try(PulsarClient client = PulsarClient.builder().serviceUrl(serviceUrl).build();
            Producer<byte[]> producer = client.newProducer().topic(topic)
                    .maxPendingMessages(getNumEntriesPerLedger() / 2).sendTimeout(60, TimeUnit.SECONDS)
                    .blockIfQueueFull(true).enableBatching(false).create();
            PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(adminUrl).build()) {

            String topicToCheck = partitionNum >= 0
                    ? topic + "-partition-" + partitionNum
                    : topic;

            List<PersistentTopicInternalStats.LedgerInfo> ledgers = admin.topics()
                    .getInternalStats(topicToCheck).ledgers;
            long currentLedger = ledgers.get(ledgers.size() - 1).ledgerId;

            client.newConsumer().topic(topic).subscriptionName("my-sub").subscribe().close();

            AtomicBoolean success = new AtomicBoolean(true);
            // write enough to topic to make it roll twice
            for (int i = 0;
                 i < getNumEntriesPerLedger() * 2.5 * (partitionNum > 0 ? partitionNum + 1 : 1);
                 i++) {
                producer.sendAsync(buildEntry("offload-message" + i))
                        .exceptionally(e -> {
                            log.error("failed to send a message", e);
                            success.set(false);
                            return null;
                        });
            }
            producer.flush();
            producer.send(buildEntry("final-offload-message"));
            Assert.assertTrue(success.get());

            // wait up to 30 seconds for offload to occur
            for (int i = 0;
                 i < 100 && !ledgerOffloaded(admin.topics().getInternalStats(topicToCheck).ledgers, currentLedger);
                 i++) {
                Thread.sleep(300);
            }
            Assert.assertTrue(ledgerOffloaded(admin.topics().getInternalStats(topicToCheck).ledgers, currentLedger));

            return currentLedger;
        }
    }

    public boolean ledgerExistsInBookKeeper(long ledgerId) throws Exception {
        ClientConfiguration bkConf = new ClientConfiguration();
        bkConf.setZkServers(pulsarCluster.getZKConnString());
        try (BookKeeperAdmin bk = new BookKeeperAdmin(bkConf)) {
            try {
                bk.openLedger(ledgerId).close();
                return true;
            } catch (BKException.BKNoSuchLedgerExistsException
                | BKException.BKNoSuchLedgerExistsOnMetadataServerException e) {
                return false;
            }
        }
    }

    protected void testPublishOffloadAndConsumeDeletionLag(String serviceUrl, String adminUrl) throws Exception {
        final String tenant = "offload-test-deletion-lag-" + randomName(4);
        final String namespace = tenant + "/ns1";
        final String topic = "persistent://" + namespace + "/topic1";

        pulsarCluster.runAdminCommandOnAnyBroker("tenants",
                "create", "--allowed-clusters", pulsarCluster.getClusterName(),
                "--admin-roles", "offload-admin", tenant);

        pulsarCluster.runAdminCommandOnAnyBroker("namespaces",
                "create", "--clusters", pulsarCluster.getClusterName(), namespace);

        // set threshold to offload runs immediately after role
        pulsarCluster.runAdminCommandOnAnyBroker("namespaces",
                "set-offload-threshold", "--size", "0", namespace);

        String output = pulsarCluster.runAdminCommandOnAnyBroker(
                "namespaces", "get-offload-deletion-lag", namespace).getStdout();
        Assert.assertTrue(output.contains("Unset for namespace"));

        long offloadedLedger = writeAndWaitForOffload(serviceUrl, adminUrl, topic);
        // give it up to 5 seconds to delete, it shouldn't
        // so we wait this every time
        Thread.sleep(5000);
        Assert.assertTrue(ledgerExistsInBookKeeper(offloadedLedger));

        pulsarCluster.runAdminCommandOnAnyBroker("namespaces", "set-offload-deletion-lag", namespace,
                "--lag", "0m");
        output = pulsarCluster.runAdminCommandOnAnyBroker(
                "namespaces", "get-offload-deletion-lag", namespace).getStdout();
        Assert.assertTrue(output.contains("0 minute(s)"));

        offloadedLedger = writeAndWaitForOffload(serviceUrl, adminUrl, topic);
        // wait up to 10 seconds for ledger to be deleted
        for (int i = 0; i < 10 && ledgerExistsInBookKeeper(offloadedLedger); i++) {
            writeAndWaitForOffload(serviceUrl, adminUrl, topic);
            Thread.sleep(1000);
        }
        Assert.assertFalse(ledgerExistsInBookKeeper(offloadedLedger));

        pulsarCluster.runAdminCommandOnAnyBroker("namespaces", "clear-offload-deletion-lag", namespace);

        Thread.sleep(5); // wait 5 seconds to allow broker to see update

        output = pulsarCluster.runAdminCommandOnAnyBroker(
                "namespaces", "get-offload-deletion-lag", namespace).getStdout();
        Assert.assertTrue(output.contains("Unset for namespace"));

        offloadedLedger = writeAndWaitForOffload(serviceUrl, adminUrl, topic);

        // give it up to 5 seconds to delete, it shouldn't
        // so we wait this every time
        Thread.sleep(5000);
        Assert.assertTrue(ledgerExistsInBookKeeper(offloadedLedger));
    }

    protected void testDeleteOffloadedTopic(String serviceUrl, String adminUrl,
                                            boolean unloadBeforeDelete, int numPartitions) throws Exception {
        final String tenant = "offload-test-cli-" + randomName(4);
        final String namespace = tenant + "/ns1";
        final String topic = "persistent://" + namespace + "/topic1";

        pulsarCluster.runAdminCommandOnAnyBroker("tenants",
                "create", "--allowed-clusters", pulsarCluster.getClusterName(),
                "--admin-roles", "offload-admin", tenant);

        pulsarCluster.runAdminCommandOnAnyBroker("namespaces",
                "create", "--clusters", pulsarCluster.getClusterName(), namespace);

        // set threshold to offload runs immediately after role
        pulsarCluster.runAdminCommandOnAnyBroker("namespaces",
                "set-offload-threshold", "--size", "0", namespace);

        pulsarCluster.runAdminCommandOnAnyBroker("namespaces",
                "set-retention", "--size", "100M", "--time", "100m", namespace);

        String output = pulsarCluster.runAdminCommandOnAnyBroker(
                "namespaces", "get-offload-deletion-lag", namespace).getStdout();
        Assert.assertTrue(output.contains("Unset for namespace"));

        if (numPartitions > 0) {
            pulsarCluster.runAdminCommandOnAnyBroker("topics",
                    "create-partitioned-topic", topic,
                    "--partitions", Integer.toString(numPartitions));
        } else {
            pulsarCluster.runAdminCommandOnAnyBroker("topics", "create", topic);
        }

        long offloadedLedger = writeAndWaitForOffload(serviceUrl, adminUrl, topic, numPartitions - 1);
        // give it up to 5 seconds to delete, it shouldn't
        // so we wait this every time
        Thread.sleep(5000);
        Assert.assertTrue(ledgerExistsInBookKeeper(offloadedLedger));

        pulsarCluster.runAdminCommandOnAnyBroker("namespaces", "set-offload-deletion-lag", namespace,
                "--lag", "0m");
        output = pulsarCluster.runAdminCommandOnAnyBroker(
                "namespaces", "get-offload-deletion-lag", namespace).getStdout();
        Assert.assertTrue(output.contains("0 minute(s)"));

        offloadedLedger = writeAndWaitForOffload(serviceUrl, adminUrl, topic, numPartitions - 1);
        // wait up to 10 seconds for ledger to be deleted
        for (int i = 0; i < 10 && ledgerExistsInBookKeeper(offloadedLedger); i++) {
            writeAndWaitForOffload(serviceUrl, adminUrl, topic, numPartitions - 1);
            Thread.sleep(1000);
        }

        Assert.assertFalse(ledgerExistsInBookKeeper(offloadedLedger));
        Assert.assertTrue(offloadedLedgerExists(topic, numPartitions - 1, offloadedLedger));

        if (unloadBeforeDelete) {
            pulsarCluster.runAdminCommandOnAnyBroker("topics", "unload", topic);
        }
        if (numPartitions > 0) {
            pulsarCluster.runAdminCommandOnAnyBroker("topics", "delete-partitioned-topic", topic);
        } else {
            pulsarCluster.runAdminCommandOnAnyBroker("topics", "delete", topic);
        }
        final long ledgerId = offloadedLedger;
        Awaitility.await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
            Assert.assertFalse(offloadedLedgerExists(topic, numPartitions - 1, ledgerId));
        });
    }

    protected void testDeleteOffloadedTopicExistsInBk(String serviceUrl, String adminUrl,
                                            boolean unloadBeforeDelete, int numPartitions) throws Exception {
        final String tenant = "offload-test-cli-" + randomName(4);
        final String namespace = tenant + "/ns1";
        final String topic = "persistent://" + namespace + "/topic1";

        pulsarCluster.runAdminCommandOnAnyBroker("tenants",
                "create", "--allowed-clusters", pulsarCluster.getClusterName(),
                "--admin-roles", "offload-admin", tenant);

        pulsarCluster.runAdminCommandOnAnyBroker("namespaces",
                "create", "--clusters", pulsarCluster.getClusterName(), namespace);

        // set threshold to offload runs immediately after role
        pulsarCluster.runAdminCommandOnAnyBroker("namespaces",
                "set-offload-threshold", "--size", "0", namespace);
        pulsarCluster.runAdminCommandOnAnyBroker("namespaces",
                "set-retention", "--size", "100M", "--time", "100m", namespace);

        if (numPartitions > 0) {
            pulsarCluster.runAdminCommandOnAnyBroker("topics",
                    "create-partitioned-topic", topic,
                    "--partitions", Integer.toString(numPartitions));
        } else {
            pulsarCluster.runAdminCommandOnAnyBroker("topics", "create", topic);
        }

        String output = pulsarCluster.runAdminCommandOnAnyBroker(
                "namespaces", "get-offload-deletion-lag", namespace).getStdout();
        Assert.assertTrue(output.contains("Unset for namespace"));

        long offloadedLedger = writeAndWaitForOffload(serviceUrl, adminUrl, topic, numPartitions - 1);
        // give it up to 5 seconds to delete, it shouldn't
        // so we wait this every time
        Thread.sleep(5000);
        Assert.assertTrue(ledgerExistsInBookKeeper(offloadedLedger));

        Assert.assertTrue(offloadedLedgerExists(topic, numPartitions - 1, offloadedLedger));

        if (unloadBeforeDelete) {
            pulsarCluster.runAdminCommandOnAnyBroker("topics", "unload", topic);
        }
        if (numPartitions > 0) {
            pulsarCluster.runAdminCommandOnAnyBroker("topics", "delete-partitioned-topic", topic);
        } else {
            pulsarCluster.runAdminCommandOnAnyBroker("topics", "delete", topic);
        }
        final long ledgerId = offloadedLedger;
        Awaitility.await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
            Assert.assertFalse(offloadedLedgerExists(topic, numPartitions - 1, ledgerId));
        });
        Assert.assertFalse(ledgerExistsInBookKeeper(offloadedLedger));
    }

    protected boolean offloadedLedgerExists(String topic, int partitionNum, long firstLedger) {
        throw new RuntimeException("not implemented");
    }
}
