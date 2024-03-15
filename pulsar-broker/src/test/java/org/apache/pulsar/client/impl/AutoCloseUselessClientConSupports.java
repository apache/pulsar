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
import org.testng.Assert;

public class AutoCloseUselessClientConSupports extends MultiBrokerBaseTest {

    protected int BROKER_COUNT = 5;

    @Override
    protected int numberOfAdditionalBrokers() {
        return BROKER_COUNT -1;
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
     * Why provide this method: prevents instability on one side
     */
    protected void connectionToEveryBrokerWithUnloadBundle(PulsarClientImpl pulsarClient){
        try {
            Awaitility.waitAtMost(Duration.ofSeconds(2)).until(() -> {
                int afterSubscribe_trans = pulsarClient.getCnxPool().getPoolSize();
                if (afterSubscribe_trans == BROKER_COUNT) {
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
     * Why provide this method: prevents instability on one side
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
     * Ensure producer and consumer works
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
     * Ensure producer and consumer works
     */
    protected void ensureProducerAndConsumerWorks(Producer producer_1, Producer producer_2, Consumer consumer)
            throws PulsarClientException, ExecutionException, InterruptedException {
        String messageContent_1 = UUID.randomUUID().toString();
        String messageContent_2 = UUID.randomUUID().toString();
        HashSet<String> expectReceived = new HashSet<>();
        expectReceived.add(messageContent_1);
        expectReceived.add(messageContent_2);
        producer_1.send(messageContent_1.getBytes(StandardCharsets.UTF_8));
        producer_2.send(messageContent_2.getBytes(StandardCharsets.UTF_8));
        Message message_1 = (Message) consumer.receiveAsync().get();
        consumer.acknowledgeAsync(message_1).get();
        Message message_2 = (Message) consumer.receiveAsync().get();
        consumer.acknowledgeAsync(message_2).get();
        HashSet<String> actualReceived = new HashSet<>();
        actualReceived.add(new String(message_1.getData(), StandardCharsets.UTF_8));
        actualReceived.add(new String(message_2.getData(), StandardCharsets.UTF_8));
        Assert.assertEquals(actualReceived, expectReceived);
    }

    /**
     * Ensure transaction works
     */
    protected void ensureTransactionWorks(PulsarClientImpl pulsarClient, Producer producer,
                                          Consumer consumer)
            throws PulsarClientException, ExecutionException, InterruptedException {
        String messageContent_before = UUID.randomUUID().toString();
        String messageContent_tx = UUID.randomUUID().toString();

        Transaction transaction = pulsarClient.newTransaction()
                .withTransactionTimeout(5, TimeUnit.MINUTES).build().get();
        // assert producer/consumer work OK
        producer.send(messageContent_before.getBytes(StandardCharsets.UTF_8));
        Message message = (Message) consumer.receiveAsync().get();
        Assert.assertEquals(new String(message.getData(), StandardCharsets.UTF_8), messageContent_before);
        producer.newMessage(transaction).value(messageContent_tx.getBytes(StandardCharsets.UTF_8)).sendAsync().get();
        consumer.acknowledgeAsync(message.getMessageId(), transaction);
        transaction.commit().get();
        Message message_tx = (Message) consumer.receiveAsync().get();
        Assert.assertEquals(new String(message_tx.getData(), StandardCharsets.UTF_8), messageContent_tx);
        consumer.acknowledge(message_tx);
    }
}
