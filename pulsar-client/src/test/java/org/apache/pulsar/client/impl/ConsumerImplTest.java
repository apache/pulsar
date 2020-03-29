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
package org.apache.pulsar.client.impl;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.netty.util.Timer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ConsumerImplTest {


    private final ExecutorService executorService = Executors.newSingleThreadExecutor();
    private ConsumerImpl<ConsumerImpl> consumer;
    private ConsumerConfigurationData consumerConf;

    @BeforeMethod
    public void setUp() {
        consumerConf = new ConsumerConfigurationData<>();
        ClientConfigurationData clientConf = new ClientConfigurationData();
        PulsarClientImpl client = mock(PulsarClientImpl.class);
        CompletableFuture<ClientCnx> clientCnxFuture = new CompletableFuture<>();
        CompletableFuture<Consumer<ConsumerImpl>> subscribeFuture = new CompletableFuture<>();
        String topic = "non-persistent://tenant/ns1/my-topic";

        // Mock connection for grabCnx()
        when(client.getConnection(anyString())).thenReturn(clientCnxFuture);
        clientConf.setOperationTimeoutMs(100);
        clientConf.setStatsIntervalSeconds(0);
        when(client.getConfiguration()).thenReturn(clientConf);
        when(client.timer()).thenReturn(mock(Timer.class));

        consumerConf.setSubscriptionName("test-sub");
        consumer = ConsumerImpl.newConsumerImpl(client, topic, consumerConf,
                executorService, -1, false, subscribeFuture, null, null, null,
                true);
    }

    @Test(invocationTimeOut = 1000)
    public void testNotifyPendingReceivedCallback_EmptyQueueNotThrowsException() {
        consumer.notifyPendingReceivedCallback(null, null);
    }

    @Test(invocationTimeOut = 500)
    public void testCorrectBackoffConfiguration() {
        final Backoff backoff = consumer.getConnectionHandler().backoff;
        ClientConfigurationData clientConfigurationData = new ClientConfigurationData();
        Assert.assertEquals(backoff.getMax(), TimeUnit.NANOSECONDS.toMillis(clientConfigurationData.getMaxBackoffIntervalNanos()));
        Assert.assertEquals(backoff.next(), TimeUnit.NANOSECONDS.toMillis(clientConfigurationData.getInitialBackoffIntervalNanos()));
    }

    @Test(invocationTimeOut = 1000)
    public void testNotifyPendingReceivedCallback_CompleteWithException() {
        CompletableFuture<Message<ConsumerImpl>> receiveFuture = new CompletableFuture<>();
        consumer.pendingReceives.add(receiveFuture);
        Exception exception = new PulsarClientException.InvalidMessageException("some random exception");
        consumer.notifyPendingReceivedCallback(null, exception);

        try {
            receiveFuture.join();
        } catch (CompletionException e) {
            // Completion exception must be the same we provided at calling time
            Assert.assertEquals(e.getCause(), exception);
        }

        Assert.assertTrue(receiveFuture.isCompletedExceptionally());
    }

    @Test(invocationTimeOut = 1000)
    public void testNotifyPendingReceivedCallback_CompleteWithExceptionWhenMessageIsNull() {
        CompletableFuture<Message<ConsumerImpl>> receiveFuture = new CompletableFuture<>();
        consumer.pendingReceives.add(receiveFuture);
        consumer.notifyPendingReceivedCallback(null, null);

        try {
            receiveFuture.join();
        } catch (CompletionException e) {
            Assert.assertEquals("received message can't be null", e.getCause().getMessage());
        }

        Assert.assertTrue(receiveFuture.isCompletedExceptionally());
    }

    @Test(invocationTimeOut = 1000)
    public void testNotifyPendingReceivedCallback_InterceptorsWorksWithPrefetchDisabled() {
        CompletableFuture<Message<ConsumerImpl>> receiveFuture = new CompletableFuture<>();
        MessageImpl message = mock(MessageImpl.class);
        ConsumerImpl<ConsumerImpl> spy = spy(consumer);

        consumer.pendingReceives.add(receiveFuture);
        consumerConf.setReceiverQueueSize(0);
        doReturn(message).when(spy).beforeConsume(any());
        spy.notifyPendingReceivedCallback(message, null);
        Message<ConsumerImpl> receivedMessage = receiveFuture.join();

        verify(spy, times(1)).beforeConsume(message);
        Assert.assertTrue(receiveFuture.isDone());
        Assert.assertFalse(receiveFuture.isCompletedExceptionally());
        Assert.assertEquals(receivedMessage, message);
    }

    @Test(invocationTimeOut = 1000)
    public void testNotifyPendingReceivedCallback_WorkNormally() {
        CompletableFuture<Message<ConsumerImpl>> receiveFuture = new CompletableFuture<>();
        MessageImpl message = mock(MessageImpl.class);
        ConsumerImpl<ConsumerImpl> spy = spy(consumer);

        consumer.pendingReceives.add(receiveFuture);
        doReturn(message).when(spy).beforeConsume(any());
        doNothing().when(spy).messageProcessed(message);
        spy.notifyPendingReceivedCallback(message, null);
        Message<ConsumerImpl> receivedMessage = receiveFuture.join();

        verify(spy, times(1)).beforeConsume(message);
        verify(spy, times(1)).messageProcessed(message);
        Assert.assertTrue(receiveFuture.isDone());
        Assert.assertFalse(receiveFuture.isCompletedExceptionally());
        Assert.assertEquals(receivedMessage, message);
    }
}
