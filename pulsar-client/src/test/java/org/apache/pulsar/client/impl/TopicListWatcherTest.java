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

import io.netty.channel.ChannelHandlerContext;
import org.apache.pulsar.client.impl.PatternMultiTopicsConsumerImpl.TopicsChangedListener;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.common.api.proto.BaseCommand;
import org.apache.pulsar.common.api.proto.CommandWatchTopicListSuccess;
import org.apache.pulsar.common.api.proto.CommandWatchTopicUpdate;
import org.apache.pulsar.common.naming.NamespaceName;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertTrue;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Pattern;

public class TopicListWatcherTest {

    private CompletableFuture<ClientCnx> clientCnxFuture;
    private TopicListWatcher watcher;
    private PulsarClientImpl client;
    private CompletableFuture<TopicListWatcher> watcherFuture;
    private TopicsChangedListener listener;

    @BeforeMethod(alwaysRun = true)
    public void setup() {
        listener = mock(TopicsChangedListener.class);
        client = mock(PulsarClientImpl.class);
        when(client.getConfiguration()).thenReturn(new ClientConfigurationData());
        clientCnxFuture = new CompletableFuture<>();
        when(client.getConnectionToServiceUrl()).thenReturn(clientCnxFuture);
        watcherFuture = new CompletableFuture<>();
        watcher = new TopicListWatcher(listener, client,
                Pattern.compile("persistent://tenant/ns/topic\\d+"), 7,
                NamespaceName.get("tenant/ns"), null, watcherFuture);
    }

    @Test
    public void testWatcherGrabsConnection() {
        verify(client).getConnectionToServiceUrl();
    }

    @Test
    public void testWatcherCreatesBrokerSideObjectWhenConnected() {
        ClientCnx clientCnx = mock(ClientCnx.class);
        CompletableFuture<CommandWatchTopicListSuccess> responseFuture = new CompletableFuture<>();
        ArgumentCaptor<BaseCommand> commandCaptor = ArgumentCaptor.forClass(BaseCommand.class);
        when(clientCnx.newWatchTopicList(any(BaseCommand.class), anyLong())).thenReturn(responseFuture);
        when(clientCnx.ctx()).thenReturn(mock(ChannelHandlerContext.class));
        clientCnxFuture.complete(clientCnx);
        verify(clientCnx).newWatchTopicList(commandCaptor.capture(), anyLong());
        CommandWatchTopicListSuccess success = new CommandWatchTopicListSuccess()
                .setWatcherId(7)
                .setRequestId(commandCaptor.getValue().getWatchTopicList().getRequestId())
                .setTopicsHash("FEED");
        success.addTopic("persistent://tenant/ns/topic11");
        responseFuture.complete(success);
        assertTrue(watcherFuture.isDone() && !watcherFuture.isCompletedExceptionally());
    }

    @Test
    public void testWatcherCallsListenerOnUpdate() {
        ClientCnx clientCnx = mock(ClientCnx.class);
        CompletableFuture<CommandWatchTopicListSuccess> responseFuture = new CompletableFuture<>();
        ArgumentCaptor<BaseCommand> commandCaptor = ArgumentCaptor.forClass(BaseCommand.class);
        when(clientCnx.newWatchTopicList(any(BaseCommand.class), anyLong())).thenReturn(responseFuture);
        when(clientCnx.ctx()).thenReturn(mock(ChannelHandlerContext.class));
        clientCnxFuture.complete(clientCnx);
        verify(clientCnx).newWatchTopicList(commandCaptor.capture(), anyLong());
        CommandWatchTopicListSuccess success = new CommandWatchTopicListSuccess()
                .setWatcherId(7)
                .setRequestId(commandCaptor.getValue().getWatchTopicList().getRequestId())
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


}
