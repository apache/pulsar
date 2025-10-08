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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.apache.pulsar.zookeeper.ZookeeperServerTest;
import org.apache.zookeeper.client.ZKClientConfig;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker")
public class ZKMetadataStoreBatchIOperationTest extends CanReconnectZKClientPulsarServiceBaseTest {

    @Override
    @BeforeClass(alwaysRun = true, timeOut = 300000)
    public void setup() throws Exception {
        System.setProperty("jute.maxbuffer", "16384");
        super.setup();
    }

    @Override
    @AfterClass(alwaysRun = true, timeOut = 300000)
    public void cleanup() throws Exception {
        System.clearProperty("jute.maxbuffer");
        super.cleanup();
    }

    protected void setConfigDefaults(ServiceConfiguration config, String clusterName,
                                     LocalBookkeeperEnsemble bookkeeperEnsemble, ZookeeperServerTest brokerConfigZk) {
        super.setConfigDefaults(config, clusterName, bookkeeperEnsemble, brokerConfigZk);
        config.setMetadataStoreBatchingEnabled(true);
        config.setMetadataStoreBatchingMaxOperations(1000);
        config.setMetadataStoreBatchingMaxSizeKb(128);
        config.setMetadataStoreBatchingMaxDelayMillis(20);
    }

    @Test(timeOut = 1000 * 60 * 2)
    public void testReceivedHugeResponse() throws Exception {
        int maxPacketLen = Integer.parseInt(System.getProperty("jute.maxbuffer",
                ZKClientConfig.CLIENT_MAX_PACKET_LENGTH_DEFAULT + ""));
        String defaultTp = BrokerTestUtil.newUniqueName("persistent://public/default/tp");
        admin.topics().createNonPartitionedTopic(defaultTp);

        int nsCount = (maxPacketLen / 834) + 1;
        log.info("Try to create {} namespaces", nsCount);
        String[] nsArray = new String[nsCount];
        String[] tpArray = new String[nsCount];
        for (int i = 0; i < nsCount; i++) {
            String ns = BrokerTestUtil.newUniqueName(defaultTenant + "/ns");
            nsArray[i] = ns;
            String tp = BrokerTestUtil.newUniqueName("persistent://" + ns + "/tp");
            tpArray[i] = tp;
            admin.namespaces().createNamespace(ns);
            admin.topics().createPartitionedTopic(tp, 16);
        }

        int len = pulsar.getLocalMetadataStore().getChildren("/managed-ledgers/" + nsArray[0] + "/persistent").join()
                .stream().mapToInt(str -> str.length()).sum();
        log.info("Packet len of list topics of per namespace: {}", len);

        long start = System.currentTimeMillis();
        CompletableFuture<Void> createSubscriptionFuture = admin.topics()
                .createSubscriptionAsync(defaultTp, "s1", MessageId.earliest);
        for (int i = 0; i < nsCount; i++) {
            pulsar.getLocalMetadataStore().getChildren("/managed-ledgers/" + nsArray[i] + "/persistent");
        }
        log.info("Send multi ZK operations in {} ms. If it is larger than 20, may can not reproduce the issue",
                (System.currentTimeMillis() - start));
        client.newConsumer().topic(defaultTp).subscriptionName("s1").subscribe().close();
        createSubscriptionFuture.get(10, TimeUnit.SECONDS);

        // cleanup.
        for (int i = 0; i < nsCount; i++) {
            admin.topics().deletePartitionedTopic(tpArray[i]);
        }
        for (int i = 0; i < nsCount; i++) {
            admin.namespaces().deleteNamespace(nsArray[i]);
        }
        admin.topics().delete(defaultTp);
    }
}
