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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertTrue;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoopGroup;
import io.netty.resolver.AddressResolver;
import io.netty.util.Timer;
import io.netty.util.TimerTask;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.regex.Pattern;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.PatternMultiTopicsConsumerImpl.TopicsChangedListener;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.metrics.InstrumentProvider;
import org.apache.pulsar.common.api.proto.CommandSuccess;
import org.apache.pulsar.common.api.proto.CommandWatchTopicListSuccess;
import org.apache.pulsar.common.api.proto.CommandWatchTopicUpdate;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.topics.TopicsPatternFactory;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class TopicListWatcherTest {

    private CompletableFuture<ClientCnx> clientCnxFuture;
    private TopicListWatcher watcher;
    private PulsarClientImpl client;
    private CompletableFuture<TopicListWatcher> watcherFuture;
    private TopicsChangedListener listener;
    private PatternMultiTopicsConsumerImpl<byte[]> patternConsumer;
    private Timer timer;

    @BeforeMethod(alwaysRun = true)
    @SuppressWarnings("unchecked")
    public void setup() {
        listener = mock(TopicsChangedListener.class);
        client = mock(PulsarClientImpl.class);
        ConnectionPool connectionPool = mock(ConnectionPool.class);
        when(client.getCnxPool()).thenReturn(connectionPool);
        when(connectionPool.genRandomKeyToSelectCon()).thenReturn(0);
        when(client.getConfiguration()).thenReturn(new ClientConfigurationData());
        clientCnxFuture = new CompletableFuture<>();
        when(client.getConnectionToServiceUrl()).thenReturn(clientCnxFuture);
        timer = mock(Timer.class);
        when(client.timer()).thenReturn(timer);
        String topic = "persistent://tenant/ns/topic\\d+";
        when(client.getConnection(anyString(), anyInt())).
                thenReturn(clientCnxFuture.thenApply(clientCnx -> Pair.of(clientCnx, false)));
        when(client.getConnection(any(), any(), anyInt())).thenReturn(clientCnxFuture);
        when(connectionPool.getConnection(any(), any(), anyInt())).thenReturn(clientCnxFuture);

        CompletableFuture<Void> completedFuture = CompletableFuture.completedFuture(null);
        patternConsumer = mock(PatternMultiTopicsConsumerImpl.class);
        when(patternConsumer.getPattern()).thenReturn(TopicsPatternFactory.create(Pattern.compile(topic)));
        when(patternConsumer.getPartitions()).thenReturn(Collections.singletonList("persistent://tenant/ns/topic11"));
        CompletableFuture<Consumer<byte[]>> subscribeFuture = CompletableFuture.completedFuture(patternConsumer);
        when(patternConsumer.getSubscribeFuture()).thenReturn(subscribeFuture);
        when(patternConsumer.recheckTopicsChange()).thenReturn(completedFuture);
        when(listener.onTopicsAdded(anyCollection())).thenReturn(completedFuture);
        when(listener.onTopicsRemoved(anyCollection())).thenReturn(completedFuture);
        when(patternConsumer.handleWatchTopicListSuccess(any(), any(), anyInt())).thenReturn(completedFuture);
        when(patternConsumer.supportsTopicListWatcherReconcile()).thenReturn(true);
        PatternConsumerUpdateQueue queue = new PatternConsumerUpdateQueue(patternConsumer, listener);

        watcherFuture = new CompletableFuture<>();
        watcher = new TopicListWatcher(queue, client,
                TopicsPatternFactory.create(Pattern.compile(topic)), 7,
                NamespaceName.get("tenant/ns"), patternConsumer::getLocalStateTopicsHash, watcherFuture,
                () -> 0);
    }

    @Test
    public void testWatcherGrabsConnection() {
        verify(client).getConnection(anyString(), anyInt());
    }

    @Test
    public void testWatcherCreatesBrokerSideObjectWhenConnected() {
        ClientCnx clientCnx = mockClientCnx();
        CompletableFuture<CommandWatchTopicListSuccess> responseFuture = new CompletableFuture<>();
        when(clientCnx.newWatchTopicList(anyLong(), anyLong(), anyString(), anyString(), any()))
                .thenReturn(responseFuture);
        when(clientCnx.ctx()).thenReturn(mock(ChannelHandlerContext.class));
        clientCnxFuture.complete(clientCnx);

        verify(clientCnx).newWatchTopicList(anyLong(), anyLong(), anyString(), anyString(), any());

        CommandWatchTopicListSuccess success = new CommandWatchTopicListSuccess()
                .setWatcherId(7)
                .setRequestId(0)
                .setTopicsHash("FEED");
        success.addTopic("persistent://tenant/ns/topic11");
        responseFuture.complete(success);
        assertTrue(watcherFuture.isDone() && !watcherFuture.isCompletedExceptionally());
    }

    private static ClientCnx mockClientCnx() {
        ClientCnx clientCnx = mock(ClientCnx.class);
        when(clientCnx.isSupportsTopicWatchers()).thenReturn(true);
        when(clientCnx.isSupportsTopicWatcherReconcile()).thenReturn(true);
        Channel channel = mock(Channel.class);
        ChannelHandlerContext context = mock(ChannelHandlerContext.class);
        when(context.channel()).thenReturn(channel);
        when(channel.isActive()).thenReturn(true);
        when(clientCnx.ctx()).thenReturn(context);
        when(clientCnx.channel()).thenReturn(channel);
        when(clientCnx.newWatchTopicListClose(any(), anyLong()))
                .thenReturn(CompletableFuture.completedFuture(new CommandSuccess()));
        return clientCnx;
    }

    @Test
    public void testRetriableInitialWatchTimeoutRecoversAfterOperationDeadline() throws Exception {
        // The pattern consumer has already subscribed before watcher creation starts. Expiring its request must
        // not permanently disable the watcher for the lifetime of that consumer.
        client.getConfiguration().setOperationTimeoutMs(0);
        ClientCnx clientCnx = mockClientCnx();
        CompletableFuture<CommandWatchTopicListSuccess> firstResponse = new CompletableFuture<>();
        CompletableFuture<CommandWatchTopicListSuccess> retriedResponse = new CompletableFuture<>();
        CompletableFuture<CommandSuccess> pendingClose = new CompletableFuture<>();
        when(clientCnx.newWatchTopicListClose(any(), anyLong())).thenReturn(pendingClose);
        when(clientCnx.newWatchTopicList(anyLong(), anyLong(), anyString(), anyString(), any()))
                .thenReturn(firstResponse, retriedResponse);
        clientCnxFuture.complete(clientCnx);

        firstResponse.completeExceptionally(new PulsarClientException.TimeoutException("Slow watch request"));

        assertThat(watcherFuture).isNotDone();
        verify(clientCnx).removeTopicListWatcher(7);
        ArgumentCaptor<TimerTask> retry = ArgumentCaptor.forClass(TimerTask.class);
        verify(timer).newTimeout(retry.capture(), anyLong(), any());
        InOrder order = inOrder(clientCnx, timer);
        order.verify(clientCnx).registerTopicListWatcher(7, watcher);
        order.verify(clientCnx).newWatchTopicList(anyLong(), anyLong(), anyString(), anyString(), any());
        order.verify(clientCnx).removeTopicListWatcher(7);
        order.verify(clientCnx).newWatchTopicListClose(any(), anyLong());
        order.verify(timer).newTimeout(any(), anyLong(), any());
        retry.getValue().run(null);
        order.verify(clientCnx).registerTopicListWatcher(7, watcher);
        retriedResponse.complete(new CommandWatchTopicListSuccess().setWatcherId(7).setRequestId(1)
                .setTopicsHash("FEED"));

        assertThat(watcherFuture).isCompletedWithValue(watcher);
        assertThat(watcher.isConnected()).isTrue();
        assertThat(pendingClose).isNotDone();
        verify(clientCnx, times(2)).newWatchTopicList(anyLong(), anyLong(), anyString(), anyString(), any());
        verify(clientCnx, times(2)).registerTopicListWatcher(7, watcher);
    }

    @Test
    public void testNonRetriableInitialWatchFailureDoesNotRetry() {
        client.getConfiguration().setOperationTimeoutMs(0);
        ClientCnx clientCnx = mockClientCnx();
        CompletableFuture<CommandWatchTopicListSuccess> response = new CompletableFuture<>();
        when(clientCnx.newWatchTopicList(anyLong(), anyLong(), anyString(), anyString(), any()))
                .thenReturn(response);
        clientCnxFuture.complete(clientCnx);

        response.completeExceptionally(new PulsarClientException.NotAllowedException("Pattern evaluation disabled"));

        assertThat(watcherFuture).isCompletedExceptionally();
        assertThat(watcher.getState()).isEqualTo(HandlerState.State.Failed);
        verify(clientCnx).removeTopicListWatcher(7);
        verify(timer, never()).newTimeout(any(), anyLong(), any());
        verify(clientCnx, never()).newWatchTopicListClose(any(), anyLong());
    }

    @Test
    public void testUnexpectedInitialWatchFailureDoesNotRetry() {
        ClientCnx clientCnx = mockClientCnx();
        CompletableFuture<CommandWatchTopicListSuccess> response = new CompletableFuture<>();
        when(clientCnx.newWatchTopicList(anyLong(), anyLong(), anyString(), anyString(), any()))
                .thenReturn(response);
        clientCnxFuture.complete(clientCnx);

        response.completeExceptionally(new IllegalStateException("Unexpected watcher creation failure"));

        assertThat(watcherFuture).isCompletedExceptionally();
        assertThat(watcher.getState()).isEqualTo(HandlerState.State.Failed);
        verify(clientCnx).removeTopicListWatcher(7);
        verify(timer, never()).newTimeout(any(), anyLong(), any());
        verify(clientCnx, never()).newWatchTopicListClose(any(), anyLong());
    }

    @Test
    public void testRetriableInitialWatchFailureSkipsCloseOnInactiveConnection() {
        ClientCnx clientCnx = mockClientCnx();
        when(clientCnx.channel().isActive()).thenReturn(false);
        CompletableFuture<CommandWatchTopicListSuccess> response = new CompletableFuture<>();
        when(clientCnx.newWatchTopicList(anyLong(), anyLong(), anyString(), anyString(), any()))
                .thenReturn(response);
        clientCnxFuture.complete(clientCnx);

        response.completeExceptionally(new PulsarClientException.TimeoutException("Disconnected watch request"));

        assertThat(watcherFuture).isNotDone();
        verify(clientCnx).removeTopicListWatcher(7);
        verify(clientCnx, never()).newWatchTopicListClose(any(), anyLong());
        verify(timer).newTimeout(any(), anyLong(), any());
    }

    @DataProvider
    public Object[][] connectionPoolSizes() {
        return new Object[][]{{0}, {1}};
    }

    @Test(dataProvider = "connectionPoolSizes")
    @SuppressWarnings("unchecked")
    public void testRetriableWatchFailuresReleaseConnections(int connectionsPerBroker) throws Exception {
        try (ConnectionPool pool = newConnectionPool(connectionsPerBroker)) {
            when(client.getCnxPool()).thenReturn(pool);
            ClientCnx firstCnx = mockClientCnx();
            ClientCnx secondCnx = mockClientCnx();
            CompletableFuture<CommandWatchTopicListSuccess> firstResponse = new CompletableFuture<>();
            CompletableFuture<CommandWatchTopicListSuccess> secondResponse = new CompletableFuture<>();
            when(firstCnx.newWatchTopicList(anyLong(), anyLong(), anyString(), anyString(), any()))
                    .thenReturn(firstResponse);
            when(secondCnx.newWatchTopicList(anyLong(), anyLong(), anyString(), anyString(), any()))
                    .thenReturn(secondResponse);
            when(secondCnx.newWatchTopicListClose(any(), anyLong()))
                    .thenThrow(new IllegalStateException("Close request could not be sent"));
            when(client.getConnection(anyString(), anyInt()))
                    .thenReturn(CompletableFuture.completedFuture(Pair.of(secondCnx, false)));
            clientCnxFuture.complete(firstCnx);

            firstResponse.completeExceptionally(new PulsarClientException.TimeoutException("First watch timeout"));
            ArgumentCaptor<TimerTask> retry = ArgumentCaptor.forClass(TimerTask.class);
            verify(timer).newTimeout(retry.capture(), anyLong(), any());
            retry.getValue().run(null);
            secondResponse.completeExceptionally(new PulsarClientException.TimeoutException("Second watch timeout"));

            int expectedCloses = connectionsPerBroker == 0 ? 1 : 0;
            InOrder order = inOrder(firstCnx, secondCnx);
            order.verify(firstCnx).newWatchTopicListClose(any(), anyLong());
            if (expectedCloses > 0) {
                order.verify(firstCnx).close();
            }
            order.verify(secondCnx).newWatchTopicListClose(any(), anyLong());
            if (expectedCloses > 0) {
                order.verify(secondCnx).close();
            }
            verify(firstCnx, times(expectedCloses)).close();
            verify(secondCnx, times(expectedCloses)).close();
            assertThat(watcherFuture).isNotDone();
            watcher.closeAsync().join();
        }
    }

    @Test
    public void testCloseStopsScheduledInitialWatchRetry() throws Exception {
        client.getConfiguration().setOperationTimeoutMs(0);
        ClientCnx clientCnx = mockClientCnx();
        CompletableFuture<CommandWatchTopicListSuccess> response = new CompletableFuture<>();
        when(clientCnx.newWatchTopicList(anyLong(), anyLong(), anyString(), anyString(), any()))
                .thenReturn(response);
        clientCnxFuture.complete(clientCnx);
        response.completeExceptionally(new PulsarClientException.TimeoutException("Slow watch request"));
        ArgumentCaptor<TimerTask> retry = ArgumentCaptor.forClass(TimerTask.class);
        verify(timer).newTimeout(retry.capture(), anyLong(), any());

        watcher.closeAsync().join();
        retry.getValue().run(null);

        assertThat(watcherFuture).isCancelled();
        assertThat(watcher.getState()).isEqualTo(HandlerState.State.Closed);
        verify(clientCnx).removeTopicListWatcher(7);
        verify(clientCnx).newWatchTopicList(anyLong(), anyLong(), anyString(), anyString(), any());
    }

    @DataProvider
    public Object[][] lateWatchResponses() {
        return new Object[][]{{0, true}, {0, false}, {1, true}, {1, false}};
    }

    @Test(dataProvider = "lateWatchResponses")
    public void testCloseCleansUpLateInitialWatchResponse(int connectionsPerBroker, boolean success) throws Exception {
        try (ConnectionPool pool = newConnectionPool(connectionsPerBroker)) {
            when(client.getCnxPool()).thenReturn(pool);
            ClientCnx clientCnx = mockClientCnx();
            CompletableFuture<CommandWatchTopicListSuccess> response = new CompletableFuture<>();
            when(clientCnx.newWatchTopicList(anyLong(), anyLong(), anyString(), anyString(), any()))
                    .thenReturn(response);
            clientCnxFuture.complete(clientCnx);

            watcher.closeAsync().join();
            if (success) {
                response.complete(new CommandWatchTopicListSuccess().setWatcherId(7).setRequestId(0)
                        .setTopicsHash("FEED"));
            } else {
                response.completeExceptionally(new PulsarClientException.TimeoutException("Late watch timeout"));
            }

            assertThat(watcherFuture).isCancelled();
            assertThat(watcher.getState()).isEqualTo(HandlerState.State.Closed);
            verify(clientCnx).removeTopicListWatcher(7);
            verify(clientCnx).newWatchTopicListClose(any(), anyLong());
            verify(clientCnx, times(connectionsPerBroker == 0 ? 1 : 0)).close();
            verify(clientCnx.channel(), never()).close();
            verify(timer, never()).newTimeout(any(), anyLong(), any());
        }
    }

    @Test(dataProvider = "connectionPoolSizes")
    public void testCloseReleasesLateConnectionWithoutCreatingWatcher(int connectionsPerBroker) throws Exception {
        try (ConnectionPool pool = newConnectionPool(connectionsPerBroker)) {
            when(client.getCnxPool()).thenReturn(pool);
            ClientCnx clientCnx = mockClientCnx();

            watcher.closeAsync().join();
            clientCnxFuture.complete(clientCnx);

            assertThat(watcherFuture).isCancelled();
            assertThat(watcher.getState()).isEqualTo(HandlerState.State.Closed);
            verify(clientCnx, never()).registerTopicListWatcher(anyLong(), any());
            verify(clientCnx, never()).newWatchTopicList(anyLong(), anyLong(), anyString(), anyString(), any());
            verify(clientCnx, never()).newWatchTopicListClose(any(), anyLong());
            verify(clientCnx, times(connectionsPerBroker == 0 ? 1 : 0)).close();
            verify(clientCnx.channel(), never()).close();
            verify(timer, never()).newTimeout(any(), anyLong(), any());
        }
    }

    @SuppressWarnings("unchecked")
    private static ConnectionPool newConnectionPool(int connectionsPerBroker) throws Exception {
        ClientConfigurationData config = new ClientConfigurationData();
        config.setConnectionsPerBroker(connectionsPerBroker);
        config.setConnectionMaxIdleSeconds(0);
        AddressResolver<InetSocketAddress> resolver = mock(AddressResolver.class);
        return new ConnectionPool(InstrumentProvider.NOOP, config, mock(EventLoopGroup.class),
                () -> mock(ClientCnx.class), Optional.of(() -> resolver), mock(ScheduledExecutorService.class));
    }

    @Test
    public void testWatcherCallsListenerOnUpdate() {
        ClientCnx clientCnx = mockClientCnx();
        CompletableFuture<CommandWatchTopicListSuccess> responseFuture = new CompletableFuture<>();
        when(clientCnx.newWatchTopicList(anyLong(), anyLong(), anyString(), anyString(), any()))
                .thenReturn(responseFuture);
        when(clientCnx.ctx()).thenReturn(mock(ChannelHandlerContext.class));
        clientCnxFuture.complete(clientCnx);
        verify(clientCnx).newWatchTopicList(anyLong(), anyLong(), anyString(), anyString(), any());

        CommandWatchTopicListSuccess success = new CommandWatchTopicListSuccess()
                .setWatcherId(7)
                .setRequestId(0)
                .setTopicsHash("FEED");
        success.addTopic("persistent://tenant/ns/topic11");
        responseFuture.complete(success);

        CommandWatchTopicUpdate update = new CommandWatchTopicUpdate()
                .setTopicsHash("F33D")
                .setWatcherId(7)
                .addAllNewTopics(Collections.singleton("persistent://tenant/ns/topic12"));

        watcher.handleCommandWatchTopicUpdate(update);
        verify(listener).onTopicsAdded(Collections.singletonList("persistent://tenant/ns/topic12"));
    }

    @Test
    public void testWatcherTriggersReconciliationOnHashMismatch() {
        ClientCnx clientCnx = mockClientCnx();

        CompletableFuture<CommandWatchTopicListSuccess> responseFuture = new CompletableFuture<>();
        when(clientCnx.newWatchTopicList(anyLong(), anyLong(), anyString(), anyString(), any()))
                .thenReturn(responseFuture);
        when(clientCnx.ctx()).thenReturn(mock(ChannelHandlerContext.class));
        clientCnxFuture.complete(clientCnx);

        CommandWatchTopicListSuccess success = new CommandWatchTopicListSuccess()
                .setWatcherId(7)
                .setRequestId(0)
                .setTopicsHash("FEED");
        success.addTopic("persistent://tenant/ns/topic11");
        responseFuture.complete(success);

        // verify that the response was handled
        verify(patternConsumer, times(1)).handleWatchTopicListSuccess(any(), any(), anyInt());
        // sync local hash
        when(patternConsumer.getLocalStateTopicsHash()).thenReturn("FEED");

        // Send update with a mismatching hash
        CommandWatchTopicUpdate update = new CommandWatchTopicUpdate()
                .setTopicsHash("WRONG_HASH")
                .setWatcherId(7)
                .addAllNewTopics(Collections.singleton("persistent://tenant/ns/topic12"));

        watcher.handleCommandWatchTopicUpdate(update);

        // Verify that reconciliation was triggered again due to hash mismatch
        verify(patternConsumer, times(1)).recheckTopicsChange();
    }
}
