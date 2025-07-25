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
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertTrue;
import io.netty.buffer.ByteBuf;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import lombok.Cleanup;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Messages;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.client.impl.conf.TopicConsumerConfigurationData;
import org.apache.pulsar.client.util.ExecutorProvider;
import org.apache.pulsar.client.util.ScheduledExecutorProvider;
import org.apache.pulsar.common.util.Backoff;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ConsumerImplTest {
    private final String topic = "non-persistent://tenant/ns1/my-topic";

    private ExecutorProvider executorProvider;
    private ExecutorService internalExecutor;
    private ConsumerImpl<byte[]> consumer;
    private ConsumerConfigurationData<byte[]> consumerConf;

    @BeforeMethod(alwaysRun = true)
    public void setUp() {
        consumerConf = new ConsumerConfigurationData<>();
        createConsumer(consumerConf);
    }

    private void createConsumer(ConsumerConfigurationData consumerConf) {
        executorProvider = new ExecutorProvider(1, "ConsumerImplTest");
        internalExecutor = Executors.newSingleThreadScheduledExecutor();

        PulsarClientImpl client = ClientTestFixtures.createPulsarClientMock(executorProvider, internalExecutor);
        ClientConfigurationData clientConf = client.getConfiguration();
        clientConf.setOperationTimeoutMs(100);
        clientConf.setStatsIntervalSeconds(0);
        CompletableFuture<Consumer<byte[]>> subscribeFuture = new CompletableFuture<>();

        consumerConf.setSubscriptionName("test-sub");
        consumer = ConsumerImpl.newConsumerImpl(client, topic, consumerConf,
                executorProvider, -1, false, subscribeFuture, null, null, null,
                true);
        consumer.setState(HandlerState.State.Ready);
    }

    @AfterMethod(alwaysRun = true)
    public void cleanup() {
        if (executorProvider != null) {
            executorProvider.shutdownNow();
            executorProvider = null;
        }
        if (internalExecutor != null) {
            internalExecutor.shutdownNow();
            internalExecutor = null;
        }
    }

    @Test(invocationTimeOut = 1000)
    public void testNotifyPendingReceivedCallback_EmptyQueueNotThrowsException() {
        consumer.notifyPendingReceivedCallback(null, null);
    }

    @Test(invocationTimeOut = 500)
    public void testCorrectBackoffConfiguration() {
        final Backoff backoff = consumer.getConnectionHandler().backoff;
        ClientConfigurationData clientConfigurationData = new ClientConfigurationData();
        Assert.assertEquals(backoff.getMax(),
                TimeUnit.NANOSECONDS.toMillis(clientConfigurationData.getMaxBackoffIntervalNanos()));
        Assert.assertEquals(backoff.next(),
                TimeUnit.NANOSECONDS.toMillis(clientConfigurationData.getInitialBackoffIntervalNanos()));
    }

    @Test(invocationTimeOut = 1000)
    public void testNotifyPendingReceivedCallback_CompleteWithException() {
        CompletableFuture<Message<byte[]>> receiveFuture = new CompletableFuture<>();
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
        CompletableFuture<Message<byte[]>> receiveFuture = new CompletableFuture<>();
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
        CompletableFuture<Message<byte[]>> receiveFuture = new CompletableFuture<>();
        MessageImpl message = mock(MessageImpl.class);
        ConsumerImpl<byte[]> spy = spy(consumer);

        consumer.pendingReceives.add(receiveFuture);
        consumerConf.setReceiverQueueSize(0);
        doReturn(message).when(spy).beforeConsume(any());
        spy.notifyPendingReceivedCallback(message, null);
        Message<byte[]> receivedMessage = receiveFuture.join();

        verify(spy, times(1)).beforeConsume(message);
        Assert.assertTrue(receiveFuture.isDone());
        Assert.assertFalse(receiveFuture.isCompletedExceptionally());
        Assert.assertEquals(receivedMessage, message);
    }

    @Test(invocationTimeOut = 1000)
    public void testNotifyPendingReceivedCallback_WorkNormally() {
        CompletableFuture<Message<byte[]>> receiveFuture = new CompletableFuture<>();
        MessageImpl message = mock(MessageImpl.class);
        ConsumerImpl<byte[]> spy = spy(consumer);

        consumer.pendingReceives.add(receiveFuture);
        doReturn(message).when(spy).beforeConsume(any());
        doNothing().when(spy).messageProcessed(message);
        spy.notifyPendingReceivedCallback(message, null);
        Message<byte[]> receivedMessage = receiveFuture.join();

        verify(spy, times(1)).beforeConsume(message);
        verify(spy, times(1)).messageProcessed(message);
        Assert.assertTrue(receiveFuture.isDone());
        Assert.assertFalse(receiveFuture.isCompletedExceptionally());
        Assert.assertEquals(receivedMessage, message);
    }

    @Test
    public void testReceiveAsyncCanBeCancelled() {
        // given
        CompletableFuture<Message<byte[]>> future = consumer.receiveAsync();
        Awaitility.await().untilAsserted(() -> Assert.assertTrue(consumer.hasNextPendingReceive()));
        // when
        future.cancel(true);
        // then
        Assert.assertTrue(consumer.pendingReceives.isEmpty());
    }

    @Test
    public void testBatchReceiveAsyncCanBeCancelled() {
        // given
        CompletableFuture<Messages<byte[]>> future = consumer.batchReceiveAsync();
        Awaitility.await().untilAsserted(() -> Assert.assertTrue(consumer.hasPendingBatchReceive()));
        // when
        future.cancel(true);
        // then
        Assert.assertFalse(consumer.hasPendingBatchReceive());
    }

    @Test
    public void testClose() {
        Exception checkException = null;
        try {
            if (consumer != null) {
                consumer.negativeAcknowledge(new MessageIdImpl(0, 0, -1));
                consumer.close();
            }
        } catch (Exception e) {
            checkException = e;
        }
        Assert.assertNull(checkException);
    }

    @Test
    public void testConsumerCreatedWhilePaused() throws InterruptedException {
        PulsarClientImpl client = ClientTestFixtures.createPulsarClientMock(executorProvider, internalExecutor);
        ClientConfigurationData clientConf = client.getConfiguration();
        clientConf.setOperationTimeoutMs(100);
        clientConf.setStatsIntervalSeconds(0);
        String topic = "non-persistent://tenant/ns1/my-topic";

        consumerConf.setStartPaused(true);

        consumer = ConsumerImpl.newConsumerImpl(client, topic, consumerConf,
                executorProvider, -1, false, new CompletableFuture<>(), null, null, null,
                true);

        Assert.assertTrue(consumer.paused);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testCreateConsumerWhenSchemaIsNull() throws PulsarClientException {
        @Cleanup
        PulsarClient client = PulsarClient.builder()
            .serviceUrl("pulsar://127.0.0.1:6650")
            .build();

        client.newConsumer(null)
            .topic("topic_testCreateConsumerWhenSchemaIsNull")
            .subscriptionName("testCreateConsumerWhenSchemaIsNull")
            .subscribe();
    }

    @Test
    public void testMaxReceiverQueueSize() {
        int size = consumer.getCurrentReceiverQueueSize();
        int permits = consumer.getAvailablePermits();
        consumer.setCurrentReceiverQueueSize(size + 100);
        Assert.assertEquals(consumer.getCurrentReceiverQueueSize(), size + 100);
        Assert.assertEquals(consumer.getAvailablePermits(), permits + 100);
    }

    @Test
    public void testTopicPriorityLevel() {
        ConsumerConfigurationData<Object> consumerConf = new ConsumerConfigurationData<>();
        consumerConf.getTopicConfigurations().add(
                TopicConsumerConfigurationData.ofTopicName(topic, 1));

        createConsumer(consumerConf);

        assertThat(consumer.getPriorityLevel()).isEqualTo(1);
    }

    @Test
    public void testSeekAsyncInternal() {
        // given
        ClientCnx cnx = mock(ClientCnx.class);
        CompletableFuture<ProducerResponse> clientReq = new CompletableFuture<>();
        when(cnx.sendRequestWithId(any(ByteBuf.class), anyLong())).thenReturn(clientReq);

        ScheduledExecutorProvider provider = mock(ScheduledExecutorProvider.class);
        ScheduledExecutorService scheduledExecutorService = mock(ScheduledExecutorService.class);
        when(provider.getExecutor()).thenReturn(scheduledExecutorService);
        when(consumer.getClient().getScheduledExecutorProvider()).thenReturn(provider);

        CompletableFuture<Void> result = consumer.seekAsync(1L);
        verify(scheduledExecutorService, atLeast(1)).schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));

        consumer.setClientCnx(cnx);
        consumer.setState(HandlerState.State.Ready);
        consumer.seekStatus.set(ConsumerImpl.SeekStatus.NOT_STARTED);

        // when
        CompletableFuture<Void> firstResult = consumer.seekAsync(1L);
        CompletableFuture<Void> secondResult = consumer.seekAsync(1L);

        clientReq.complete(null);

        assertTrue(firstResult.isDone());
        assertTrue(secondResult.isCompletedExceptionally());
        verify(cnx, times(1)).sendRequestWithId(any(ByteBuf.class), anyLong());
    }

    @Test(invocationTimeOut = 1000)
    public void testAutoGenerateConsumerName() {
        Pattern consumerNamePattern = Pattern.compile("[a-zA-Z0-9]{5}");
        assertTrue(consumerNamePattern.matcher(consumer.getConsumerName()).matches());
    }
}
