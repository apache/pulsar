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

import com.google.common.collect.Sets;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.util.Timer;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.client.util.ExecutorProvider;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.util.netty.EventLoopUtil;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import java.net.SocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

/**
 * Unit Tests of {@link MultiTopicsConsumerImpl}.
 */
public class MultiTopicsConsumerImplTest {

    @Test
    public void testGetStats() throws Exception {
        String topicName = "test-stats";
        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setServiceUrl("pulsar://localhost:6650");
        conf.setStatsIntervalSeconds(100);

        ThreadFactory threadFactory = new DefaultThreadFactory("client-test-stats", Thread.currentThread().isDaemon());
        EventLoopGroup eventLoopGroup = EventLoopUtil.newEventLoopGroup(conf.getNumIoThreads(), threadFactory);
        ExecutorService listenerExecutor = Executors.newSingleThreadScheduledExecutor(threadFactory);

        PulsarClientImpl clientImpl = new PulsarClientImpl(conf, eventLoopGroup);

        ConsumerConfigurationData consumerConfData = new ConsumerConfigurationData();
        consumerConfData.setTopicNames(Sets.newHashSet(topicName));

        assertEquals(Long.parseLong("100"), clientImpl.getConfiguration().getStatsIntervalSeconds());

        MultiTopicsConsumerImpl impl = new MultiTopicsConsumerImpl(
            clientImpl, consumerConfData,
            listenerExecutor, null, null, null, true);

        impl.getStats();
    }

    // Test uses a mocked PulsarClientImpl which will complete the getPartitionedTopicMetadata() internal async call
    // after a delay longer than the interval between the two subscribeAsync() calls in the test method body.
    //
    // Code under tests is using CompletableFutures. Theses may hang indefinitely if code is broken.
    // That's why a test timeout is defined.
    @Test(timeOut = 5000)
    public void testParallelSubscribeAsync() throws Exception {
        String topicName = "parallel-subscribe-async-topic";
        String subscriptionName = "parallel-subscribe-async-subscription";
        String serviceUrl = "pulsar://localhost:6650";
        Schema<byte[]> schema = Schema.BYTES;
        ExecutorService listenerExecutor = mock(ExecutorService.class);
        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setServiceUrl(serviceUrl);
        ConsumerConfigurationData<byte[]> consumerConfData = new ConsumerConfigurationData<>();
        consumerConfData.setSubscriptionName(subscriptionName);
        int completionDelayMillis = 100;
        PulsarClientImpl client = setUpPulsarClientMock(schema, completionDelayMillis);
        MultiTopicsConsumerImpl<byte[]> impl = new MultiTopicsConsumerImpl<>(client, consumerConfData, listenerExecutor,
            new CompletableFuture<>(), schema, null, true);

        CompletableFuture<Void> firstInvocation = impl.subscribeAsync(topicName, true);
        Thread.sleep(5); // less than completionDelayMillis
        CompletableFuture<Void> secondInvocation = impl.subscribeAsync(topicName, true);

        firstInvocation.get(); // does not throw
        Throwable t = expectThrows(ExecutionException.class, secondInvocation::get);
        Throwable cause = t.getCause();
        assertEquals(cause.getClass(), PulsarClientException.class);
        assertTrue(cause.getMessage().endsWith("Topic is already being subscribed for in other thread."));
    }

    private <T> PulsarClientImpl setUpPulsarClientMock(Schema<T> schema, int completionDelayMillis) {
        PulsarClientImpl clientMock = mock(PulsarClientImpl.class, Mockito.RETURNS_DEEP_STUBS);

        when(clientMock.getConfiguration()).thenReturn(mock(ClientConfigurationData.class));
        when(clientMock.timer()).thenReturn(mock(Timer.class));
        when(clientMock.getPartitionedTopicMetadata(any())).thenAnswer(invocation -> createDelayedCompletedFuture(
            new PartitionedTopicMetadata(), completionDelayMillis));
        when(clientMock.<T>preProcessSchemaBeforeSubscribe(any(), any(), any()))
            .thenReturn(CompletableFuture.completedFuture(schema));
        when(clientMock.externalExecutorProvider()).thenReturn(mock(ExecutorProvider.class));
        when(clientMock.eventLoopGroup().next()).thenReturn(mock(EventLoop.class));

        ClientCnx clientCnxMock = mock(ClientCnx.class, Mockito.RETURNS_DEEP_STUBS);
        when(clientCnxMock.ctx()).thenReturn(mock(ChannelHandlerContext.class));
        when(clientCnxMock.sendRequestWithId(any(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(mock(ProducerResponse.class)));
        when(clientCnxMock.channel().remoteAddress()).thenReturn(mock(SocketAddress.class));
        when(clientMock.getConnection(any())).thenReturn(CompletableFuture.completedFuture(clientCnxMock));

        return clientMock;
    }

    private <T> CompletableFuture<T> createDelayedCompletedFuture(T result, int delayMillis) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                Thread.sleep(delayMillis);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            return result;
        });
    }
}
