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

import static org.assertj.core.api.Assertions.assertThat;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashSet;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.MultiBrokerBaseTest;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.awaitility.Awaitility;
import org.awaitility.reflect.WhiteboxImpl;
import org.testng.Assert;

public abstract class AutoCloseUselessClientConSupports extends MultiBrokerBaseTest {

    protected static final int BROKER_COUNT = 5;

    @Override
    protected int numberOfAdditionalBrokers() {
        return BROKER_COUNT - 1;
    }

    @Override
    protected PulsarClient newPulsarClient(String url, int intervalInSecs) throws PulsarClientException {
        ClientBuilder clientBuilder =
                PulsarClient.builder()
                        .serviceUrl(url)
                        .connectionMaxIdleSeconds(60)
                        .statsInterval(intervalInSecs, TimeUnit.SECONDS);
        customizeNewPulsarClientBuilder(clientBuilder);
        return createNewPulsarClient(clientBuilder);
    }

    /**
     * To ensure that connection release is triggered in a short time, call manually.
     * Why provide this method: prevents instability on one side
     */
    protected void trigReleaseConnection(PulsarClientImpl pulsarClient)
            throws InterruptedException, NoSuchFieldException, IllegalAccessException {
        // Wait for every request has been response.
        Awaitility.waitAtMost(Duration.ofSeconds(5)).until(() -> {
            for (CompletableFuture<ClientCnx> clientCnxWrapFuture : pulsarClient.getCnxPool().getConnections()){
                if (!clientCnxWrapFuture.isDone()){
                    continue;
                }
                ClientCnx clientCnx = clientCnxWrapFuture.getNow(null);
                if (clientCnx == null){
                    continue;
                }
                if (clientCnx.getPendingRequests().size() > 0) {
                    return false;
                }
            }
            return true;
        });
        // Shorten max idle time
        pulsarClient.getCnxPool().connectionMaxIdleSeconds = 1;
        // trig : mark useless connections
        pulsarClient.getCnxPool().doMarkAndReleaseUselessConnections();
        // trig : after 2 seconds, release useless connections
        Thread.sleep(2000);
        pulsarClient.getCnxPool().doMarkAndReleaseUselessConnections();
    }

    /**
     * Create connections to all brokers using the "Unload Bundle"
     * If can't create it in a short time, use direct implements {@link #connectionToEveryBroker}
     * Why provide this method: prevents instability on one side.
     */
    protected void connectionToEveryBrokerWithUnloadBundle(PulsarClientImpl pulsarClient){
        try {
            Awaitility.waitAtMost(Duration.ofSeconds(2)).until(() -> {
                int afterSubscribeTrans = pulsarClient.getCnxPool().getPoolSize();
                if (afterSubscribeTrans == BROKER_COUNT) {
                    return true;
                }
                for (PulsarAdmin pulsarAdmin : super.getAllAdmins()) {
                    pulsarAdmin.namespaces().unloadNamespaceBundle("public/default", "0x00000000_0xffffffff");
                    Thread.sleep(20);
                }
                return false;
            });
        } catch (Exception e){
            connectionToEveryBroker(pulsarClient);
        }
    }

    /**
     * Create connections directly to all brokers
     * Why provide this method: prevents instability on one side.
     */
    protected void connectionToEveryBroker(PulsarClientImpl pulsarClient){
        for (PulsarService pulsarService : super.getAllBrokers()){
            String url = pulsarService.getBrokerServiceUrl();
            if (url.contains("//")){
                url = url.split("//")[1];
            }
            String host = url;
            int port = 6650;
            if (url.contains(":")){
                String[] hostAndPort = url.split(":");
                host = hostAndPort[0];
                port = Integer.valueOf(hostAndPort[1]);
            }
            pulsarClient.getCnxPool()
                    .getConnection(new InetSocketAddress(host, port));
        }
    }

    /**
     * Ensure producer and consumer works.
     */
    protected void ensureProducerAndConsumerWorks(Producer producer, Consumer consumer)
            throws PulsarClientException, ExecutionException, InterruptedException {
        String messageContent = UUID.randomUUID().toString();
        producer.send(messageContent.getBytes(StandardCharsets.UTF_8));
        Message message = (Message) consumer.receiveAsync().get();
        consumer.acknowledgeAsync(message).get();
        Assert.assertEquals(new String(message.getData(), StandardCharsets.UTF_8), messageContent);
    }

    /**
     * Ensure producer and consumer works.
     */
    protected void ensureProducerAndConsumerWorks(Producer producer1, Producer producer2, Consumer consumer)
            throws PulsarClientException, ExecutionException, InterruptedException {
        String messageContent1 = UUID.randomUUID().toString();
        String messageContent2 = UUID.randomUUID().toString();
        HashSet<String> expectReceived = new HashSet<>();
        expectReceived.add(messageContent1);
        expectReceived.add(messageContent2);
        producer1.send(messageContent1.getBytes(StandardCharsets.UTF_8));
        producer2.send(messageContent2.getBytes(StandardCharsets.UTF_8));
        Message message1 = (Message) consumer.receiveAsync().get();
        consumer.acknowledgeAsync(message1).get();
        Message message2 = (Message) consumer.receiveAsync().get();
        consumer.acknowledgeAsync(message2).get();
        HashSet<String> actualReceived = new HashSet<>();
        actualReceived.add(new String(message1.getData(), StandardCharsets.UTF_8));
        actualReceived.add(new String(message2.getData(), StandardCharsets.UTF_8));
        Assert.assertEquals(actualReceived, expectReceived);
    }

    /**
     * Ensure transaction works.
     */
    protected void ensureTransactionWorks(PulsarClientImpl pulsarClient, Producer producer,
                                          Consumer consumer)
            throws PulsarClientException, ExecutionException, InterruptedException {
        String messageContentBefore = UUID.randomUUID().toString();
        String messageContentTx = UUID.randomUUID().toString();

        Transaction transaction = pulsarClient.newTransaction()
                .withTransactionTimeout(5, TimeUnit.MINUTES).build().get();
        // assert producer/consumer work OK
        producer.send(messageContentBefore.getBytes(StandardCharsets.UTF_8));
        Message message = (Message) consumer.receiveAsync().get();
        Assert.assertEquals(new String(message.getData(), StandardCharsets.UTF_8), messageContentBefore);
        producer.newMessage(transaction).value(messageContentTx.getBytes(StandardCharsets.UTF_8)).sendAsync().get();
        consumer.acknowledgeAsync(message.getMessageId(), transaction);
        transaction.commit().get();
        Message messageTx = (Message) consumer.receiveAsync().get();
        Assert.assertEquals(new String(messageTx.getData(), StandardCharsets.UTF_8), messageContentTx);
        consumer.acknowledge(messageTx);
    }

    protected void waitForTopicListWatcherStarted(Consumer<?> consumer) {
        Awaitility.await().untilAsserted(() -> {
            CompletableFuture<TopicListWatcher> completableFuture =
                    WhiteboxImpl.getInternalState(consumer, "watcherFuture");
            assertThat(completableFuture).describedAs("Topic list watcher future should be done").isDone();
        });
    }
}
