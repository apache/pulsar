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
package org.apache.pulsar.broker.service;

import java.lang.reflect.Field;
import java.net.URL;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.util.StringUtils;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.ClientConfiguration;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.PropertyAdmin;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 */
public class BrokerBkEnsemblesTests {
    protected static int BROKER_SERVICE_PORT = 16650;
    PulsarService pulsar;
    ServiceConfiguration config;

    URL adminUrl;
    PulsarAdmin admin;

    LocalBookkeeperEnsemble bkEnsemble;

    private final int ZOOKEEPER_PORT = 12759;
    protected final int BROKER_WEBSERVICE_PORT = 15782;

    @BeforeMethod
    void setup() throws Exception {
        try {
            // start local bookie and zookeeper
            bkEnsemble = new LocalBookkeeperEnsemble(3, ZOOKEEPER_PORT, 5001);
            bkEnsemble.start();

            // start pulsar service
            config = new ServiceConfiguration();
            config.setZookeeperServers("127.0.0.1" + ":" + ZOOKEEPER_PORT);
            config.setWebServicePort(BROKER_WEBSERVICE_PORT);
            config.setClusterName("usc");
            config.setBrokerServicePort(BROKER_SERVICE_PORT);
            config.setAuthorizationEnabled(false);
            config.setAuthenticationEnabled(false);
            config.setManagedLedgerMaxEntriesPerLedger(5);
            config.setManagedLedgerMinLedgerRolloverTimeMinutes(0);

            pulsar = new PulsarService(config);
            pulsar.start();

            adminUrl = new URL("http://127.0.0.1" + ":" + BROKER_WEBSERVICE_PORT);
            admin = new PulsarAdmin(adminUrl, (Authentication) null);

            admin.clusters().createCluster("usc", new ClusterData(adminUrl.toString()));
            admin.properties().createProperty("prop",
                    new PropertyAdmin(Lists.newArrayList("appid1"), Sets.newHashSet("usc")));
        } catch (Throwable t) {
            LOG.error("Error setting up broker test", t);
            Assert.fail("Broker test setup failed");
        }
    }

    @AfterMethod
    void shutdown() throws Exception {
        try {
            admin.close();
            pulsar.close();
            bkEnsemble.stop();
        } catch (Throwable t) {
            LOG.error("Error cleaning up broker test setup state", t);
            Assert.fail("Broker test cleanup failed");
        }
    }

    /**
     * It verifies that broker deletes cursor-ledger when broker-crashes without closing topic gracefully
     * 
     * <pre>
     * 1. Create topic : publish/consume-ack msgs to update new cursor-ledger
     * 2. Verify cursor-ledger is created and ledger-znode present
     * 3. Broker crashes: remove topic and managed-ledgers without closing
     * 4. Recreate topic: publish/consume-ack msgs to update new cursor-ledger
     * 5. Topic is recovered from old-ledger and broker deletes the old ledger
     * 6. verify znode of old-ledger is deleted
     * </pre>
     * 
     * @throws Exception
     */
    @Test
    public void testCrashBrokerWithoutCursorLedgerLeak() throws Exception {

        ZooKeeper zk = bkEnsemble.getZkClient();
        ClientConfiguration clientConf = new ClientConfiguration();
        clientConf.setStatsInterval(0, TimeUnit.SECONDS);
        PulsarClient client = PulsarClient.create(adminUrl.toString(), clientConf);

        final String ns1 = "prop/usc/crash-broker";

        admin.namespaces().createNamespace(ns1);

        final String dn1 = "persistent://" + ns1 + "/my-topic";

        // (1) create topic
        // publish and ack messages so, cursor can create cursor-ledger and update metadata
        Consumer consumer = client.subscribe(dn1, "my-subscriber-name");
        Producer producer = client.createProducer(dn1);
        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }
        Message msg = null;
        for (int i = 0; i < 10; i++) {
            msg = consumer.receive(1, TimeUnit.SECONDS);
            consumer.acknowledge(msg);
        }

        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getTopic(dn1).get();
        ManagedCursorImpl cursor = (ManagedCursorImpl) topic.getManagedLedger().getCursors().iterator().next();
        retryStrategically((test) -> cursor.getState().equals("Open"), 5, 100);

        // (2) validate cursor ledger is created and znode is present
        long cursorLedgerId = cursor.getCursorLedger();
        String ledgerPath = "/ledgers" + StringUtils.getHybridHierarchicalLedgerPath(cursorLedgerId);
        Assert.assertNotNull(zk.exists(ledgerPath, false));

        // (3) remove topic and managed-ledger from broker which means topic is not closed gracefully
        consumer.close();
        producer.close();
        pulsar.getBrokerService().removeTopicFromCache(dn1);
        ManagedLedgerFactoryImpl factory = (ManagedLedgerFactoryImpl) pulsar.getManagedLedgerFactory();
        Field field = ManagedLedgerFactoryImpl.class.getDeclaredField("ledgers");
        field.setAccessible(true);
        @SuppressWarnings("unchecked")
        ConcurrentHashMap<String, CompletableFuture<ManagedLedgerImpl>> ledgers = (ConcurrentHashMap<String, CompletableFuture<ManagedLedgerImpl>>) field
                .get(factory);
        ledgers.clear();

        // (4) Recreate topic
        // publish and ack messages so, cursor can create cursor-ledger and update metadata
        consumer = client.subscribe(dn1, "my-subscriber-name");
        producer = client.createProducer(dn1);
        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }
        for (int i = 0; i < 10; i++) {
            msg = consumer.receive(1, TimeUnit.SECONDS);
            consumer.acknowledge(msg);
        }

        // (5) Broker should create new cursor-ledger and remove old cursor-ledger
        topic = (PersistentTopic) pulsar.getBrokerService().getTopic(dn1).get();
        final ManagedCursorImpl cursor1 = (ManagedCursorImpl) topic.getManagedLedger().getCursors().iterator().next();
        retryStrategically((test) -> cursor1.getState().equals("Open"), 5, 100);
        long newCursorLedgerId = cursor1.getCursorLedger();
        Assert.assertNotEquals(newCursorLedgerId, -1);
        Assert.assertNotEquals(cursorLedgerId, newCursorLedgerId);

        // cursor node must be deleted
        Assert.assertNull(zk.exists(ledgerPath, false));

        producer.close();
        consumer.close();
        client.close();

    }

    void retryStrategically(Predicate<Void> predicate, int retryCount, long intSleepTime) throws Exception {
        for (int i = 0; i < retryCount; i++) {
            if (predicate.test(null) || i == (retryCount - 1)) {
                break;
            }
            Thread.sleep(intSleepTime + (intSleepTime * i));
        }
    }

    private static final Logger LOG = LoggerFactory.getLogger(BrokerBkEnsemblesTests.class);
}
