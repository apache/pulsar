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
package org.apache.pulsar.client.impl;

import com.google.common.collect.Sets;
import io.netty.channel.ChannelHandlerContext;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.PulsarVersion;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.api.proto.ServerError;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.protocol.Commands;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "broker-impl")
public class ClientCnxTest extends MockedPulsarServiceBaseTest {

    public static final String CLUSTER_NAME = "test";
    public static final String TENANT = "tnx";
    public static final String NAMESPACE = TENANT + "/ns1";
    public static String persistentTopic = "persistent://" + NAMESPACE + "/test";
    ExecutorService executorService;

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        admin.clusters().createCluster(CLUSTER_NAME, ClusterData.builder()
                .serviceUrl(pulsar.getWebServiceAddress()).build());
        admin.tenants().createTenant(TENANT,
                new TenantInfoImpl(Sets.newHashSet("appid1"), Sets.newHashSet(CLUSTER_NAME)));
        admin.namespaces().createNamespace(NAMESPACE);
        executorService = Executors.newFixedThreadPool(20);
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
        this.executorService.shutdownNow();
    }

    @Test
    public void testRemoveAndHandlePendingRequestInCnx() throws Exception {

        String subName = "sub";
        int operationTimes = 5000;
        CountDownLatch countDownLatch = new CountDownLatch(operationTimes);

        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(persistentTopic)
                .subscriptionName(subName)
                .subscribe();

        new Thread(() -> {
            for (int i = 0; i < operationTimes; i++) {
                executorService.submit(() -> {
                    consumer.getLastMessageIdAsync().whenComplete((ignore, exception) -> {
                        countDownLatch.countDown();
                    });
                });
            }
        }).start();

        for (int i = 0; i < operationTimes; i++) {
            ClientCnx cnx = ((ConsumerImpl<?>) consumer).getClientCnx();
            if (cnx != null) {
                ChannelHandlerContext context = cnx.ctx();
                if (context != null) {
                    cnx.ctx().close();
                }
            }
        }

        Awaitility.await().until(() -> {
            countDownLatch.await();
            return true;
        });

    }

    @Test
    public void testClientVersion() throws Exception {
        final String expectedVersion = String.format("Pulsar-Java-v%s", PulsarVersion.getVersion());
        final String topic = "persistent://" + NAMESPACE + "/testClientVersion";

        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topic)
                .create();

        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .subscriptionName("my-sub")
                .topic(topic)
                .subscribe();

        Assert.assertEquals(admin.topics().getStats(topic).getPublishers().get(0).getClientVersion(), expectedVersion);
        Assert.assertEquals(admin.topics().getStats(topic).getSubscriptions().get("my-sub").getConsumers().get(0)
                .getClientVersion(), expectedVersion);

        producer.close();
        consumer.close();
    }

    @Test
    public void testCnxReceiveSendError() throws Exception {
        final String topicOne = "persistent://" + NAMESPACE + "/testCnxReceiveSendError-one";
        final String topicTwo = "persistent://" + NAMESPACE + "/testCnxReceiveSendError-two";

        Producer<String> producerOne = pulsarClient.newProducer(Schema.STRING)
                .topic(topicOne)
                .create();
        Producer<String> producerTwo = pulsarClient.newProducer(Schema.STRING)
                .topic(topicTwo)
                .create();
        ClientCnx cnxOne = ((ProducerImpl<?>) producerOne).getClientCnx();
        ClientCnx cnxTwo = ((ProducerImpl<?>) producerTwo).getClientCnx();

        // simulate a sending error
        cnxOne.handleSendError(Commands.newSendErrorCommand(((ProducerImpl<?>) producerOne).producerId,
                10, ServerError.PersistenceError, "persistent error").getSendError());

        // two producer use the same producer
        Assert.assertEquals(cnxOne, cnxTwo);

        // the cnx will not change
        try {
            Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() ->
                    !cnxOne.equals(((ProducerImpl<?>) producerOne).getClientCnx())
                            || !cnxTwo.equals(((ProducerImpl<?>) producerOne).getClientCnx()));
            Assert.fail();
        } catch (Throwable e) {
            Assert.assertTrue(e instanceof ConditionTimeoutException);
        }

        Assert.assertEquals(cnxOne, cnxTwo);

        // producer also can send message
        producerOne.send("test");
        producerTwo.send("test");
        producerTwo.close();
        producerOne.close();
    }
}
