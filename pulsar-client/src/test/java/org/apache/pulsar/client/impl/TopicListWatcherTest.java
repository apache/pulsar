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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertTrue;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timer;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Pattern;
import lombok.Cleanup;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.impl.PatternMultiTopicsConsumerImpl.TopicsChangedListener;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.common.api.proto.CommandWatchTopicListSuccess;
import org.apache.pulsar.common.api.proto.CommandWatchTopicUpdate;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.topics.TopicsPatternFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TopicListWatcherTest {

    private CompletableFuture<ClientCnx> clientCnxFuture;
    private TopicListWatcher watcher;
    private PulsarClientImpl client;
    private CompletableFuture<TopicListWatcher> watcherFuture;
    private TopicsChangedListener listener;
    private PatternMultiTopicsConsumerImpl<byte[]> patternConsumer;

    @BeforeMethod(alwaysRun = true)
    public void setup() {
        listener = mock(TopicsChangedListener.class);
        client = mock(PulsarClientImpl.class);
        ConnectionPool connectionPool = mock(ConnectionPool.class);
        when(client.getCnxPool()).thenReturn(connectionPool);
        when(connectionPool.genRandomKeyToSelectCon()).thenReturn(0);
        when(client.getConfiguration()).thenReturn(new ClientConfigurationData());
        clientCnxFuture = new CompletableFuture<>();
        when(client.getConnectionToServiceUrl()).thenReturn(clientCnxFuture);
        @Cleanup("stop")
        Timer timer = new HashedWheelTimer();
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
        ClientCnx clientCnx = mock(ClientCnx.class);
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

    @Test
    public void testWatcherCallsListenerOnUpdate() {
        ClientCnx clientCnx = mock(ClientCnx.class);
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
        ClientCnx clientCnx = mock(ClientCnx.class);

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
