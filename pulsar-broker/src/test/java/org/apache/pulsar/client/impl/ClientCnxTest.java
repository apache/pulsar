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
import java.lang.reflect.Field;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.PulsarVersion;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.service.ServerCnx;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageIdAdv;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
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

@Slf4j
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
        final String topicThree = "persistent://" + NAMESPACE + "/testCnxReceiveSendError-three";

        PulsarClient client = PulsarClient.builder().serviceUrl(lookupUrl.toString()).connectionsPerBroker(1).build();
        Producer<String> producerOne = client.newProducer(Schema.STRING)
                .topic(topicOne)
                .create();
        Producer<String> producerTwo = client.newProducer(Schema.STRING)
                .topic(topicTwo)
                .create();
        Producer<String> producerThree = client.newProducer(Schema.STRING)
                .topic(topicThree).producerName("three")
                .create();
        ClientCnx cnxOne = ((ProducerImpl<?>) producerOne).getClientCnx();
        ClientCnx cnxTwo = ((ProducerImpl<?>) producerTwo).getClientCnx();
        ClientCnx cnxThree = ((ProducerImpl<?>) producerTwo).getClientCnx();

        // simulate a sending error
        cnxOne.handleSendError(Commands.newSendErrorCommand(((ProducerImpl<?>) producerOne).producerId,
                10, ServerError.PersistenceError, "persistent error 1").getSendError());
        cnxThree.handleSendError(Commands.newSendErrorCommand(((ProducerImpl<?>) producerOne).producerId,
                10, ServerError.PersistenceError, "persistent error 3").getSendError());

        // two producer use the same cnx
        Assert.assertEquals(cnxOne, cnxTwo);
        Assert.assertEquals(cnxThree, cnxTwo);

        // the cnx will not change
        try {
            Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() ->
                    (((ProducerImpl<?>) producerOne).getClientCnx() != null
                            && !cnxOne.equals(((ProducerImpl<?>) producerOne).getClientCnx()))
                    ||  (((ProducerImpl<?>) producerThree).getClientCnx() != null
                            && !cnxThree.equals(((ProducerImpl<?>) producerThree).getClientCnx()))
                    || !cnxTwo.equals(((ProducerImpl<?>) producerTwo).getClientCnx()));
            Assert.fail();
        } catch (Throwable e) {
            Assert.assertTrue(e instanceof ConditionTimeoutException);
        }

        // two producer use the same cnx
        Assert.assertEquals(((ProducerImpl<?>) producerTwo).getClientCnx(),
                ((ProducerImpl<?>) producerOne).getClientCnx());

        // producer also can send message
        producerOne.send("test");
        producerTwo.send("test");
        producerThree.send("test");
        producerTwo.close();
        producerOne.close();
        producerThree.close();
        client.close();
    }

    @Test
    public void testCnxReceiveSendErrorWithMultiConnectionsPerBroker() throws Exception {
        PulsarClient client = PulsarClient.builder().serviceUrl(lookupUrl.toString())
                .connectionsPerBroker(1000).build();

        // Create a producer with customized name.
        final String tp = BrokerTestUtil.newUniqueName(NAMESPACE + "/tp");
        admin.topics().createNonPartitionedTopic(tp);
        ProducerImpl<String> p =
                (ProducerImpl<String>) client.newProducer(Schema.STRING).producerName("p1").topic(tp).create();

        // Inject a persistence error.
        org.apache.pulsar.broker.service.Producer serverProducer = pulsar.getBrokerService().getTopic(tp, false)
                .join().get().getProducers().values().iterator().next();
        ServerCnx serverCnx = (ServerCnx) serverProducer.getCnx();
        serverCnx.getCommandSender().sendSendError(serverProducer.getProducerId(), 1/* sequenceId */,
                ServerError.PersistenceError, "mocked error");

        // Wait for the client receives the error.
        // If the client confirmed two Pings, it means the client has handled the PersistenceError we sent.
        serverCnx.checkConnectionLiveness().join();
        serverCnx.checkConnectionLiveness().join();

        try {
            // Verify: the next publish will finish.
            MessageId messageId = p.sendAsync("1").get(10, TimeUnit.SECONDS);
            MessageIdAdv messageIdAdv = (MessageIdAdv) messageId;
            log.info("sent {}:{}", messageIdAdv.getLedgerId(), messageIdAdv.getEntryId());
        } finally {
            // cleanup orphan producers.
            serverCnx.ctx().close();
            // cleanup
            client.close();
            p.close();
            admin.topics().delete(tp);
        }
    }

    public void testSupportsGetPartitionedMetadataWithoutAutoCreation() throws Exception {
        final String topic = BrokerTestUtil.newUniqueName( "persistent://" + NAMESPACE + "/tp");
        admin.topics().createNonPartitionedTopic(topic);
        PulsarClientImpl clientWitBinaryLookup = (PulsarClientImpl) PulsarClient.builder()
                .maxNumberOfRejectedRequestPerConnection(1)
                .connectionMaxIdleSeconds(Integer.MAX_VALUE)
                .serviceUrl(pulsar.getBrokerServiceUrl())
                .build();
        ProducerImpl producer = (ProducerImpl) clientWitBinaryLookup.newProducer().topic(topic).create();

        // Verify: the variable "isSupportsGetPartitionedMetadataWithoutAutoCreation" responded from the broker is true.
        Awaitility.await().untilAsserted(() -> {
            ClientCnx clientCnx = producer.getClientCnx();
            Assert.assertNotNull(clientCnx);
            Assert.assertTrue(clientCnx.isSupportsGetPartitionedMetadataWithoutAutoCreation());
        });
        Assert.assertEquals(
                clientWitBinaryLookup.getPartitionsForTopic(topic, true).get().size(), 1);

        // Inject a "false" value for the variable "isSupportsGetPartitionedMetadataWithoutAutoCreation".
        // Verify: client will get a not support error.
        Field field = ClientCnx.class.getDeclaredField("supportsGetPartitionedMetadataWithoutAutoCreation");
        field.setAccessible(true);
        for (CompletableFuture<ClientCnx> clientCnxFuture : clientWitBinaryLookup.getCnxPool().getConnections()) {
            field.set(clientCnxFuture.get(), false);
        }
        try {
            clientWitBinaryLookup.getPartitionsForTopic(topic, false).join();
            Assert.fail("Expected an error that the broker version is too old.");
        } catch (Exception ex) {
            Assert.assertTrue(ex.getMessage().contains("without auto-creation is not supported by the broker"));
        }

        // cleanup.
        producer.close();
        clientWitBinaryLookup.close();
        admin.topics().delete(topic, false);
    }
}
