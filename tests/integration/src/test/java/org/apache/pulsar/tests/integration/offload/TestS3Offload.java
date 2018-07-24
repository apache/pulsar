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
package org.apache.pulsar.tests.integration.offload;

import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.conf.ClientConfiguration;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;

import org.apache.pulsar.tests.integration.containers.BrokerContainer;
import org.apache.pulsar.tests.integration.containers.S3Container;
import org.apache.pulsar.tests.integration.topologies.PulsarCluster;
import org.apache.pulsar.tests.integration.topologies.PulsarClusterSpec;
import org.apache.pulsar.tests.integration.topologies.PulsarClusterTestBase;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static java.util.stream.Collectors.joining;

@Slf4j
public class TestS3Offload extends PulsarClusterTestBase {

    private static final int ENTRY_SIZE = 1024;
    private static final int ENTRIES_PER_LEDGER = 1024;

    @Override
    @BeforeClass
    public void setupCluster() throws Exception {

        final String clusterName = Stream.of(this.getClass().getSimpleName(), randomName(5))
                .filter(s -> s != null && !s.isEmpty())
                .collect(joining("-"));

        PulsarClusterSpec spec = PulsarClusterSpec.builder()
            .numBookies(2)
            .numBrokers(1)
            .externalServices(ImmutableMap.of(S3Container.NAME, new S3Container(clusterName, S3Container.NAME)))
            .clusterName(clusterName)
            .build();

        log.info("Setting up cluster {} with {} bookies, {} brokers",
                spec.clusterName(), spec.numBookies(), spec.numBrokers());

        pulsarCluster = PulsarCluster.forSpec(spec);

        for(BrokerContainer brokerContainer : pulsarCluster.getBrokers()){
            brokerContainer.withEnv("managedLedgerMaxEntriesPerLedger", String.valueOf(ENTRIES_PER_LEDGER));
            brokerContainer.withEnv("managedLedgerMinLedgerRolloverTimeMinutes", "0");
            brokerContainer.withEnv("managedLedgerOffloadDriver", "s3");
            brokerContainer.withEnv("s3ManagedLedgerOffloadBucket", "pulsar-integtest");
            brokerContainer.withEnv("s3ManagedLedgerOffloadServiceEndpoint", "http://" + S3Container.NAME + ":9090");
        }

        pulsarCluster.start();

        log.info("Cluster {} is setup", spec.clusterName());
    }

    private static byte[] buildEntry(String pattern) {
        byte[] entry = new byte[ENTRY_SIZE];
        byte[] patternBytes = pattern.getBytes();

        for (int i = 0; i < entry.length; i++) {
            entry[i] = patternBytes[i % patternBytes.length];
        }
        return entry;
    }

    @Test(dataProvider =  "ServiceAndAdminUrls")
    public void testPublishOffloadAndConsumeViaCLI(String serviceUrl, String adminUrl) throws Exception {
        final String tenant = "s3-offload-test-cli-" + randomName(4);
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
            Producer producer = client.newProducer().topic(topic)
                .blockIfQueueFull(true).enableBatching(false).create();
            PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(adminUrl).build()) {
            client.newConsumer().topic(topic).subscriptionName("my-sub").subscribe().close();

            // write enough to topic to make it roll
            int i = 0;
            for (; i < ENTRIES_PER_LEDGER * 1.5; i++) {
                producer.sendAsync(buildEntry("offload-message" + i));
            }
            MessageId latestMessage = producer.send(buildEntry("offload-message" + i));

            // read managed ledger info, check ledgers exist
            firstLedger = admin.topics().getInternalStats(topic).ledgers.get(0).ledgerId;

            // first offload with a high threshold, nothing should offload

            String output = pulsarCluster.runAdminCommandOnAnyBroker("topics",
                    "offload", "--size-threshold", "100G", topic).getStdout();
            Assert.assertTrue(output.contains("Nothing to offload"));

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
        }

        // stop brokers to clear all caches, open handles, etc
        pulsarCluster.stopAllBrokers();

        // delete the first ledger, so that we cannot possibly read from it
        ClientConfiguration bkConf = new ClientConfiguration();
        bkConf.setZkServers(pulsarCluster.getZKConnString());
        try (BookKeeper bk = new BookKeeper(bkConf)) {
            bk.deleteLedger(firstLedger);
        }

        // start all brokers again
        pulsarCluster.startAllBrokers();

        log.info("Read back the data (which would be in that first ledger)");
        try(PulsarClient client = PulsarClient.builder().serviceUrl(serviceUrl).build();
            Consumer consumer = client.newConsumer().topic(topic).subscriptionName("my-sub").subscribe()) {
            // read back from topic
            for (int i = 0; i < ENTRIES_PER_LEDGER * 1.5; i++) {
                Message m = consumer.receive(1, TimeUnit.MINUTES);
                Assert.assertEquals(buildEntry("offload-message" + i), m.getData());
            }
        }
    }

    @Test(dataProvider =  "ServiceAndAdminUrls")
    public void testPublishOffloadAndConsumeViaThreshold(String serviceUrl, String adminUrl) throws Exception {
        final String tenant = "s3-offload-test-threshold-" + randomName(4);
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
            Producer producer = client.newProducer().topic(topic)
                .blockIfQueueFull(true).enableBatching(false).create();
            PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(adminUrl).build()) {

            client.newConsumer().topic(topic).subscriptionName("my-sub").subscribe().close();

            // write enough to topic to make it roll twice
            for (int i = 0; i < ENTRIES_PER_LEDGER * 2.5; i++) {
                producer.sendAsync(buildEntry("offload-message" + i));
            }
            producer.send(buildEntry("final-offload-message"));

            firstLedger = admin.topics().getInternalStats(topic).ledgers.get(0).ledgerId;

            // wait up to 30 seconds for offload to occur
            for (int i = 0; i < 300 && !admin.topics().getInternalStats(topic).ledgers.get(0).offloaded; i++) {
                Thread.sleep(100);
            }
            Assert.assertTrue(admin.topics().getInternalStats(topic).ledgers.get(0).offloaded);
        }

        // stop brokers to clear all caches, open handles, etc
        pulsarCluster.stopAllBrokers();

        // delete the first ledger, so that we cannot possibly read from it
        ClientConfiguration bkConf = new ClientConfiguration();
        bkConf.setZkServers(pulsarCluster.getZKConnString());
        try (BookKeeper bk = new BookKeeper(bkConf)) {
            bk.deleteLedger(firstLedger);
        }

        // start all brokers again
        pulsarCluster.startAllBrokers();

        log.info("Read back the data (which would be in that first ledger)");
        try (PulsarClient client = PulsarClient.builder().serviceUrl(serviceUrl).build();
             Consumer consumer = client.newConsumer().topic(topic).subscriptionName("my-sub").subscribe()) {
            // read back from topic
            for (int i = 0; i < ENTRIES_PER_LEDGER * 2.5; i++) {
                Message m = consumer.receive(1, TimeUnit.MINUTES);
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
}
