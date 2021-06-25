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

import com.google.common.collect.Sets;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import lombok.Cleanup;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.ClusterDataImpl;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class MaxMessageSizeTest {

    PulsarService pulsar;
    ServiceConfiguration configuration;

    PulsarAdmin admin;

    LocalBookkeeperEnsemble bkEnsemble;

    @BeforeMethod
    void setup() {
        try {
            bkEnsemble = new LocalBookkeeperEnsemble(3, 0, () -> 0);
            ServerConfiguration conf = new ServerConfiguration();
            conf.setNettyMaxFrameSizeBytes(10 * 1024 * 1024);
            bkEnsemble.startStandalone(conf, false);

            configuration = new ServiceConfiguration();
            configuration.setZookeeperServers("127.0.0.1:" + bkEnsemble.getZookeeperPort());
            configuration.setAdvertisedAddress("localhost");
            configuration.setWebServicePort(Optional.of(0));
            configuration.setClusterName("max_message_test");
            configuration.setBrokerShutdownTimeoutMs(0L);
            configuration.setBrokerServicePort(Optional.of(0));
            configuration.setAuthorizationEnabled(false);
            configuration.setAuthenticationEnabled(false);
            configuration.setManagedLedgerMaxEntriesPerLedger(5);
            configuration.setManagedLedgerMinLedgerRolloverTimeMinutes(0);
            configuration.setMaxMessageSize(10 * 1024 * 1024);

            pulsar = new PulsarService(configuration);
            pulsar.start();

            String url = "http://127.0.0.1:" + pulsar.getListenPortHTTP().get();
            admin = PulsarAdmin.builder().serviceHttpUrl(url).build();
            admin.clusters().createCluster("max_message_test", ClusterData.builder().serviceUrl(url).build());
            admin.tenants()
                 .createTenant("test", new TenantInfoImpl(Sets.newHashSet("appid1"), Sets.newHashSet("max_message_test")));
            admin.namespaces().createNamespace("test/message", Sets.newHashSet("max_message_test"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @AfterMethod(alwaysRun = true)
    void shutdown() {
        try {
            pulsar.close();
            bkEnsemble.stop();
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }

    @Test
    public void testMaxMessageSetting() throws PulsarClientException {

        @Cleanup
        PulsarClient client = PulsarClient.builder().serviceUrl(pulsar.getBrokerServiceUrl()).build();
        String topicName = "persistent://test/message/topic1";
        Producer producer = client.newProducer().topic(topicName).sendTimeout(60, TimeUnit.SECONDS).create();
        Consumer consumer = client.newConsumer().topic(topicName).subscriptionName("test1").subscribe();

        // less than 5MB message

        byte[] normalMsg = new byte[2 * 1024 * 1024];

        try {
            producer.send(normalMsg);
        } catch (PulsarClientException e) {
            Assert.fail("Shouldn't have exception at here", e);
        }

        byte[] consumerNormalMsg = consumer.receive().getData();
        Assert.assertEquals(normalMsg, consumerNormalMsg);

        // equal 5MB message
        byte[] limitMsg = new byte[5 * 1024 * 1024];
        try {
            producer.send(limitMsg);
        } catch (PulsarClientException e) {
            Assert.fail("Shouldn't have exception at here", e);
        }

        byte[] consumerLimitMsg = consumer.receive().getData();
        Assert.assertEquals(limitMsg, consumerLimitMsg);

        // less than 10MB message
        byte[] newNormalMsg = new byte[8 * 1024 * 1024];
        try {
            producer.send(newNormalMsg);
        } catch (PulsarClientException e) {
            Assert.fail("Shouldn't have exception at here", e);
        }

        byte[] consumerNewNormalMsg = consumer.receive().getData();
        Assert.assertEquals(newNormalMsg, consumerNewNormalMsg);

        // equals 10MB message
        byte[] newLimitMsg = new byte[10 * 1024 * 1024];
        try {
            producer.send(newLimitMsg);
            Assert.fail("Shouldn't send out this message");
        } catch (PulsarClientException e) {
            // no-op
        }

        consumer.unsubscribe();
        consumer.close();
        producer.close();

    }
}
