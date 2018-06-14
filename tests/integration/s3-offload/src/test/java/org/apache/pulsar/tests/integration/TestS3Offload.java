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
import com.google.common.collect.ImmutableMap;

import java.net.URL;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.conf.ClientConfiguration;

import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.ManagedLedgerInfo;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;

import org.apache.pulsar.client.admin.LongRunningProcessStatus;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;

import org.apache.pulsar.tests.DockerUtils;
import org.apache.pulsar.tests.PulsarClusterUtils;
import org.jboss.arquillian.test.api.ArquillianResource;
import org.jboss.arquillian.testng.Arquillian;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TestS3Offload extends Arquillian {
    private static final Logger log = LoggerFactory.getLogger(TestS3Offload.class);

    private static final String CLUSTER_NAME = "test";
    private static final int ENTRY_SIZE = 1024;
    private static final int ENTRIES_PER_LEDGER = 1024;

    @ArquillianResource
    DockerClient docker;

    @BeforeMethod
    public void configureAndStartBrokers() throws Exception {

        String s3ip = DockerUtils.cubeIdsWithLabels(
                docker, ImmutableMap.of("service", "s3", "cluster", CLUSTER_NAME))
            .stream().map((c) -> DockerUtils.getContainerIP(docker, c)).findFirst().get();

        String brokerConfFile = "/pulsar/conf/broker.conf";
        for (String b : PulsarClusterUtils.brokerSet(docker, CLUSTER_NAME)) {
            PulsarClusterUtils.updateConf(docker, b, brokerConfFile,
                    "managedLedgerMaxEntriesPerLedger", String.valueOf(ENTRIES_PER_LEDGER));
            PulsarClusterUtils.updateConf(docker, b, brokerConfFile,
                    "managedLedgerMinLedgerRolloverTimeMinutes", "0");
            PulsarClusterUtils.updateConf(docker, b, brokerConfFile,
                    "managedLedgerOffloadDriver", "s3");
            PulsarClusterUtils.updateConf(docker, b, brokerConfFile,
                    "s3ManagedLedgerOffloadBucket", "pulsar-integtest");
            PulsarClusterUtils.updateConf(docker, b, brokerConfFile,
                    "s3ManagedLedgerOffloadServiceEndpoint", "http://" + s3ip + ":9090");
        }

        Assert.assertTrue(PulsarClusterUtils.startAllBrokers(docker, CLUSTER_NAME));
        Assert.assertTrue(PulsarClusterUtils.startAllProxies(docker, CLUSTER_NAME));
    }

    @AfterMethod
    public void teardownBrokers() throws Exception {
        PulsarClusterUtils.stopAllProxies(docker, CLUSTER_NAME);
        Assert.assertTrue(PulsarClusterUtils.stopAllBrokers(docker, CLUSTER_NAME));

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
        PulsarClusterUtils.runOnAnyBroker(docker, CLUSTER_NAME,
                "/pulsar/bin/pulsar-admin", "tenants",
                "create", "--allowed-clusters", CLUSTER_NAME,
                "--admin-roles", "offload-admin", "s3-offload-test");
        PulsarClusterUtils.runOnAnyBroker(docker, CLUSTER_NAME,
                "/pulsar/bin/pulsar-admin", "namespaces",
                "create", "--clusters", CLUSTER_NAME, "s3-offload-test/ns1");

        String broker = PulsarClusterUtils.brokerSet(docker, CLUSTER_NAME).stream().findAny().get();
        String brokerIp = DockerUtils.getContainerIP(docker, broker);
        String proxyIp  = PulsarClusterUtils.proxySet(docker, CLUSTER_NAME)
            .stream().map((c) -> DockerUtils.getContainerIP(docker, c)).findFirst().get();
        String serviceUrl = "pulsar://" + proxyIp + ":6650";
        String adminUrl = "http://" + brokerIp + ":8080";
        String topic = "persistent://s3-offload-test/ns1/topic1";

        ClientConfiguration bkConf = new ClientConfiguration();
        bkConf.setZkServers(PulsarClusterUtils.zookeeperConnectString(docker, CLUSTER_NAME));

        long firstLedger = -1;
        try(PulsarClient client = PulsarClient.builder().serviceUrl(serviceUrl).build();
            Producer producer = client.newProducer().topic(topic)
                .blockIfQueueFull(true).enableBatching(false).create()) {
            client.subscribe(topic, "my-sub").close();

            // write enough to topic to make it roll
            int i = 0;
            for (; i < ENTRIES_PER_LEDGER*1.5; i++) {
                producer.sendAsync(buildEntry("offload-message"+i));
            }
            MessageId latestMessage = producer.send(buildEntry("offload-message"+i));

            // read managed ledger info, check ledgers exist
            ManagedLedgerFactory mlf = new ManagedLedgerFactoryImpl(bkConf);
            ManagedLedgerInfo info = mlf.getManagedLedgerInfo("s3-offload-test/ns1/persistent/topic1");
            Assert.assertEquals(info.ledgers.size(), 2);

            firstLedger = info.ledgers.get(0).ledgerId;

            // first offload with a high threshold, nothing should offload
            String output = DockerUtils.runCommand(docker, broker,
                    "/pulsar/bin/pulsar-admin", "topics",
                    "offload", "--size-threshold", "100G",
                    topic);
            Assert.assertTrue(output.contains("Nothing to offload"));

            output = DockerUtils.runCommand(docker, broker,
                    "/pulsar/bin/pulsar-admin", "topics", "offload-status", topic);
            Assert.assertTrue(output.contains("Offload has not been run"));

            // offload with a low threshold
            output = DockerUtils.runCommand(docker, broker,
                    "/pulsar/bin/pulsar-admin", "topics",
                    "offload", "--size-threshold", "1M",
                    topic);
            Assert.assertTrue(output.contains("Offload triggered"));

            output = DockerUtils.runCommand(docker, broker,
                    "/pulsar/bin/pulsar-admin", "topics", "offload-status", "-w", topic);
            Assert.assertTrue(output.contains("Offload was a success"));
        }

        log.info("Kill ledger");
        // stop brokers to clear all caches, open handles, etc
        Assert.assertTrue(PulsarClusterUtils.stopAllBrokers(docker, CLUSTER_NAME));

        // delete the first ledger, so that we cannot possibly read from it
        try (BookKeeper bk = new BookKeeper(bkConf)) {
            bk.deleteLedger(firstLedger);
        }

        // start all brokers again
        Assert.assertTrue(PulsarClusterUtils.startAllBrokers(docker, CLUSTER_NAME));

        log.info("Read back the data (which would be in that first ledger)");
        try(PulsarClient client = PulsarClient.builder().serviceUrl(serviceUrl).build();
            Consumer consumer = client.newConsumer().topic(topic).subscriptionName("my-sub").subscribe()) {
            // read back from topic
            for (int i = 0; i < ENTRIES_PER_LEDGER*1.5; i++) {
                Message m = consumer.receive(1, TimeUnit.MINUTES);
                Assert.assertEquals(buildEntry("offload-message"+i), m.getData());
            }
        }
    }
}
