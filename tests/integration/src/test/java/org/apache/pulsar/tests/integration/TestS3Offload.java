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
package org.apache.pulsar.tests.integration;

import com.github.dockerjava.api.DockerClient;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.conf.ClientConfiguration;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;

import org.apache.pulsar.tests.DockerUtils;
import org.apache.pulsar.tests.PulsarClusterUtils;

import org.apache.pulsar.tests.integration.cluster.Cluster2Bookie1BrokerWithS3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TestS3Offload {
    private static final Logger LOG = LoggerFactory.getLogger(TestS3Offload.class);
    private static final String CLUSTER_NAME = "test";
    private static final int ENTRY_SIZE = 1024;
    private static final int ENTRIES_PER_LEDGER = 1024;
    private DockerClient docker;
    private Cluster2Bookie1BrokerWithS3 cluster;

    @BeforeClass
    public void setup() throws ExecutionException {
        cluster = new Cluster2Bookie1BrokerWithS3(TestS3Offload.class.getSimpleName());
        cluster.start();
        docker = cluster.getDockerClient();
    }

    @BeforeMethod
    public void configureAndStartBrokers() throws Exception {

        final String brokerConfFile = "/pulsar/conf/broker.conf";
        cluster.updateBrokerConf(brokerConfFile,
                    "managedLedgerMaxEntriesPerLedger",
                    String.valueOf(ENTRIES_PER_LEDGER));
        cluster.updateBrokerConf(brokerConfFile,
                    "managedLedgerMinLedgerRolloverTimeMinutes", "0");
        cluster.updateBrokerConf(brokerConfFile,
                    "managedLedgerOffloadDriver", "s3");
        cluster.updateBrokerConf(brokerConfFile,
                    "s3ManagedLedgerOffloadBucket", "pulsar-integtest");
        cluster.updateBrokerConf(brokerConfFile,
                    "s3ManagedLedgerOffloadServiceEndpoint", "http://" + "s3" + ":9090");

        cluster.startAllBrokers();
        cluster.startAllProxies();

    }

    @AfterMethod
    public void teardownBrokers() throws Exception {
        cluster.stopAllBrokers();
        cluster.stopAllProxies();
//        PulsarClusterUtils.stopAllProxies(docker, CLUSTER_NAME);
//        Assert.assertTrue(PulsarClusterUtils.stopAllBrokers(docker, CLUSTER_NAME));
    }

    private static byte[] buildEntry(String pattern) {
        byte[] entry = new byte[ENTRY_SIZE];
        byte[] patternBytes = pattern.getBytes();

        for (int i = 0; i < entry.length; i++) {
            entry[i] = patternBytes[i % patternBytes.length];
        }
        return entry;
    }

    @Test
    public void testPublishOffloadAndConsumeViaCLI() throws Exception {
        final String TENANT = "s3-offload-test-cli";
        final String NAMESPACE = "s3-offload-test-cli/ns1";
        final String TOPIC = "persistent://s3-offload-test-cli/ns1/topic1";

        cluster.execInBroker(
                "/pulsar/bin/pulsar-admin", "tenants",
                "create", "--allowed-clusters", CLUSTER_NAME,
                "--admin-roles", "offload-admin", TENANT);
        cluster.execInBroker(
                "/pulsar/bin/pulsar-admin", "namespaces",
                "create", "--clusters", CLUSTER_NAME, NAMESPACE);

//        String broker = PulsarClusterUtils.brokerSet(docker, CLUSTER_NAME).stream().findFirst().get();
//        String proxyIp = PulsarClusterUtils.proxySet(docker, CLUSTER_NAME)
//            .stream().map((c) -> DockerUtils.getContainerIP(docker, c)).findFirst().get();
        String serviceUrl = "pulsar://" + cluster.getPulsarProxyIP() + ":6650";
        String adminUrl = "http://" + cluster.getPulsarProxyIP()  + ":8080";

        long firstLedger = -1;
        try(PulsarClient client = PulsarClient.builder().serviceUrl(serviceUrl).build();
            Producer producer = client.newProducer().topic(TOPIC)
                .blockIfQueueFull(true).enableBatching(false).create();
            PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(adminUrl).build()) {
            client.newConsumer().topic(TOPIC).subscriptionName("my-sub").subscribe().close();

            // write enough to topic to make it roll
            int i = 0;
            for (; i < ENTRIES_PER_LEDGER*1.5; i++) {
                producer.sendAsync(buildEntry("offload-message"+i));
            }
            MessageId latestMessage = producer.send(buildEntry("offload-message"+i));

            // read managed ledger info, check ledgers exist
            firstLedger = admin.topics().getInternalStats(TOPIC).ledgers.get(0).ledgerId;

            // first offload with a high threshold, nothing should offload
            String output = cluster.execInBroker(
                    "/pulsar/bin/pulsar-admin", "topics",
                    "offload", "--size-threshold", "100G", TOPIC);
            Assert.assertTrue(output.contains("Nothing to offload"));

            output = cluster.execInBroker(
                    "/pulsar/bin/pulsar-admin", "topics", "offload-status", TOPIC);
            Assert.assertTrue(output.contains("Offload has not been run"));

            // offload with a low threshold
            output = cluster.execInBroker(
                    "/pulsar/bin/pulsar-admin", "topics",
                    "offload", "--size-threshold", "1M", TOPIC);
            Assert.assertTrue(output.contains("Offload triggered"));

            output = cluster.execInBroker(
                    "/pulsar/bin/pulsar-admin", "topics", "offload-status", "-w", TOPIC);
            Assert.assertTrue(output.contains("Offload was a success"));
        }

        // stop brokers to clear all caches, open handles, etc
//        Assert.assertTrue(PulsarClusterUtils.stopAllBrokers(docker, CLUSTER_NAME));

        cluster.stopAllBrokers();

        // delete the first ledger, so that we cannot possibly read from it
        ClientConfiguration bkConf = new ClientConfiguration();
        bkConf.setZkServers(cluster.getZkConnectionString());
        try (BookKeeper bk = new BookKeeper(bkConf)) {
            bk.deleteLedger(firstLedger);
        }

//        // start all brokers again
//        Assert.assertTrue(PulsarClusterUtils.startAllBrokers(docker, CLUSTER_NAME));
        cluster.startAllBrokers();

        LOG.info("Read back the data (which would be in that first ledger)");
        try(PulsarClient client = PulsarClient.builder().serviceUrl(serviceUrl).build();
            Consumer consumer = client.newConsumer().topic(TOPIC).subscriptionName("my-sub").subscribe()) {
            // read back from topic
            for (int i = 0; i < ENTRIES_PER_LEDGER*1.5; i++) {
                Message m = consumer.receive(1, TimeUnit.MINUTES);
                Assert.assertEquals(buildEntry("offload-message"+i), m.getData());
            }
        }
    }

    @Test
    public void testPublishOffloadAndConsumeViaThreshold() throws Exception {
        final String TENANT = "s3-offload-test-threshold";
        final String NAMESPACE = "s3-offload-test-threshold/ns1";
        final String TOPIC = "persistent://s3-offload-test-threshold/ns1/topic1";

        cluster.execInBroker(
                "/pulsar/bin/pulsar-admin", "tenants",
                "create", "--allowed-clusters", CLUSTER_NAME,
                "--admin-roles", "offload-admin", TENANT);
        cluster.execInBroker(
                "/pulsar/bin/pulsar-admin", "namespaces",
                "create", "--clusters", CLUSTER_NAME, NAMESPACE);
        cluster.execInBroker(
                "/pulsar/bin/pulsar-admin", "namespaces",
                "set-offload-threshold", "--size", "1M", NAMESPACE);

//        String proxyIp  = PulsarClusterUtils.proxySet(docker, CLUSTER_NAME)
//            .stream().map((c) -> DockerUtils.getContainerIP(docker, c)).findFirst().get();
        String serviceUrl = "pulsar://" + cluster.getPulsarProxyIP() + ":" + 6650;
        String adminUrl = "http://" + cluster.getPulsarProxyIP() + ":" + 8080;

        long firstLedger = 0;
        try(PulsarClient client = PulsarClient.builder().serviceUrl(serviceUrl).build();
            Producer producer = client.newProducer().topic(TOPIC)
                .blockIfQueueFull(true).enableBatching(false).create();
            PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(adminUrl).build()) {

            client.newConsumer().topic(TOPIC).subscriptionName("my-sub").subscribe().close();

            // write enough to topic to make it roll twice
            for (int i = 0; i < ENTRIES_PER_LEDGER*2.5; i++) {
                producer.sendAsync(buildEntry("offload-message"+i));
            }
            producer.send(buildEntry("final-offload-message"));

            firstLedger = admin.topics().getInternalStats(TOPIC).ledgers.get(0).ledgerId;

            // wait up to 30 seconds for offload to occur
            for (int i = 0; i < 300 && !admin.topics().getInternalStats(TOPIC).ledgers.get(0).offloaded; i++) {
                Thread.sleep(100);
            }
            Assert.assertTrue(admin.topics().getInternalStats(TOPIC).ledgers.get(0).offloaded);
        }

        // stop brokers to clear all caches, open handles, etc
        //Assert.assertTrue(PulsarClusterUtils.stopAllBrokers(docker, CLUSTER_NAME));
        cluster.stopAllBrokers();

        cluster.stopAllBrokers();

        // delete the first ledger, so that we cannot possibly read from it
        ClientConfiguration bkConf = new ClientConfiguration();
        bkConf.setZkServers(cluster.getZkConnectionString());
        try (BookKeeper bk = new BookKeeper(bkConf)) {
            bk.deleteLedger(firstLedger);
        }

        // start all brokers again
//        Assert.assertTrue(PulsarClusterUtils.startAllBrokers(docker, CLUSTER_NAME));

        cluster.startAllBrokers();

        LOG.info("Read back the data (which would be in that first ledger)");
        try (PulsarClient client = PulsarClient.builder().serviceUrl(serviceUrl).build();
             Consumer consumer = client.newConsumer().topic(TOPIC).subscriptionName("my-sub").subscribe()) {
            // read back from topic
            for (int i = 0; i < ENTRIES_PER_LEDGER*2.5; i++) {
                Message m = consumer.receive(1, TimeUnit.MINUTES);
                Assert.assertEquals(buildEntry("offload-message"+i), m.getData());
            }
        }

        // try disabling
        cluster.execInBroker(
                "/pulsar/bin/pulsar-admin", "namespaces",
                "set-offload-threshold", "--size", "-1", NAMESPACE);

        // hard to validate that it has been disabled as we'd be waiting for
        // something _not_ to happen (i.e. waiting for ages), so just check
        try (PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(adminUrl).build()) {
            Assert.assertEquals(admin.namespaces().getOffloadThreshold(NAMESPACE), -1L);
        }
    }
}
