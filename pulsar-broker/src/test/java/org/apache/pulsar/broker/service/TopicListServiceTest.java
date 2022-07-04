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
package org.apache.pulsar.broker.service;

import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.broker.resources.PulsarResources;
import org.apache.pulsar.broker.resources.TopicResources;
import org.apache.pulsar.common.api.proto.CommandWatchTopicListClose;
import org.apache.pulsar.common.api.proto.ServerError;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.topics.TopicList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;


import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.regex.Pattern;

public class TopicListServiceTest {

    private TopicListService topicListService;
    private ServerCnx connection;
    private CompletableFuture<List<String>> topicListFuture;
    private Semaphore lookupSemaphore;
    private TopicResources topicResources;

    @BeforeMethod(alwaysRun = true)
    public void setup() throws Exception {
        lookupSemaphore = new Semaphore(1);
        lookupSemaphore.acquire();
        topicListFuture = new CompletableFuture<>();
        topicResources = mock(TopicResources.class);

        PulsarService pulsar = mock(PulsarService.class);
        when(pulsar.getNamespaceService()).thenReturn(mock(NamespaceService.class));
        when(pulsar.getPulsarResources()).thenReturn(mock(PulsarResources.class));
        when(pulsar.getPulsarResources().getTopicResources()).thenReturn(topicResources);
        when(pulsar.getNamespaceService().getListOfPersistentTopics(any())).thenReturn(topicListFuture);


        connection = mock(ServerCnx.class);
        when(connection.getRemoteAddress()).thenReturn(new InetSocketAddress(10000));
        when(connection.getCommandSender()).thenReturn(mock(PulsarCommandSender.class));

        topicListService = new TopicListService(pulsar, connection, true, 30);

    }

    @Test
    public void testCommandWatchSuccessResponse() {

        topicListService.handleWatchTopicList(
                NamespaceName.get("tenant/ns"),
                13,
                7,
                Pattern.compile("persistent://tenant/ns/topic\\d"),
                null,
                lookupSemaphore);
        List<String> topics = Collections.singletonList("persistent://tenant/ns/topic1");
        String hash = TopicList.calculateHash(topics);
        topicListFuture.complete(topics);
        Assert.assertEquals(1, lookupSemaphore.availablePermits());
        verify(topicResources).registerPersistentTopicListener(
                eq(NamespaceName.get("tenant/ns")), any(TopicListService.TopicListWatcher.class));
        verify(connection.getCommandSender()).sendWatchTopicListSuccess(7, 13, hash, topics);
    }

    @Test
    public void testCommandWatchErrorResponse() {
        topicListService.handleWatchTopicList(
                NamespaceName.get("tenant/ns"),
                13,
                7,
                Pattern.compile("persistent://tenant/ns/topic\\d"),
                null,
                lookupSemaphore);
        topicListFuture.completeExceptionally(new PulsarServerException("Error"));
        Assert.assertEquals(1, lookupSemaphore.availablePermits());
        verifyNoInteractions(topicResources);
        verify(connection.getCommandSender()).sendErrorResponse(eq(7L), any(ServerError.class),
                eq(PulsarServerException.class.getCanonicalName() + ": Error"));
    }

    @Test
    public void testCommandWatchTopicListCloseRemovesListener() {
        topicListService.handleWatchTopicList(
                NamespaceName.get("tenant/ns"),
                13,
                7,
                Pattern.compile("persistent://tenant/ns/topic\\d"),
                null,
                lookupSemaphore);
        List<String> topics = Collections.singletonList("persistent://tenant/ns/topic1");
        topicListFuture.complete(topics);

        CommandWatchTopicListClose watchTopicListClose = new CommandWatchTopicListClose()
                .setRequestId(8)
                .setWatcherId(13);
        topicListService.handleWatchTopicListClose(watchTopicListClose);
        verify(topicResources).deregisterPersistentTopicListener(any(TopicListService.TopicListWatcher.class));
    }

}
