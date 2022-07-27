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
package org.apache.pulsar.websocket;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.vertx.core.impl.ConcurrentHashSet;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import javax.servlet.http.HttpServletRequest;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.ConsumerBuilderImpl;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.websocket.data.ConsumerCommand;
import org.apache.pulsar.websocket.data.ConsumerMessage;
import org.apache.pulsar.websocket.service.WebSocketProxyConfiguration;
import org.awaitility.Awaitility;
import org.eclipse.jetty.websocket.api.RemoteEndpoint;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.WriteCallback;
import org.eclipse.jetty.websocket.servlet.ServletUpgradeResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

public class ConsumerHandlerTest {
    private static final Logger log = LoggerFactory.getLogger(ConsumerHandlerTest.class);

    @Test
    @SuppressWarnings("unchecked")
    public void pendingMessagesTest() throws Exception {
        final TopicName topicName = TopicName.get("persistent://my-tenant/my-ns/pending-messages-test");
        final String consumeV2 = "/ws/v2/consumer/" + topicName.getRestPath() + "/my-sub";
        final Map<String, String[]> queryParams = new HashMap<String, String>(){{
            put("receiverQueueSize", "2");
        }}.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, entry -> new String[]{ entry.getValue() }));

        final HttpServletRequest httpServletRequest = mock(HttpServletRequest.class);
        when(httpServletRequest.getRequestURI()).thenReturn(consumeV2);
        when(httpServletRequest.getParameterMap()).thenReturn(queryParams);

        final WebSocketService service = mock(WebSocketService.class);
        final ServiceConfiguration config = PulsarConfigurationLoader.convertFrom(
                PulsarConfigurationLoader.create(
                        this.getClass().getClassLoader().getResource("websocket.conf").getFile(),
                        WebSocketProxyConfiguration.class));
        config.setAuthenticationEnabled(false);
        config.setAuthorizationEnabled(false);
        final Session session = mock(Session.class);
        final ServletUpgradeResponse response = mock(ServletUpgradeResponse.class);
        final RemoteEndpoint remoteEndpoint = mock(RemoteEndpoint.class);
        final PulsarClientImpl client = mock(PulsarClientImpl.class);
        final ConsumerBuilder<byte[]> builder = spy(new ConsumerBuilderImpl<>(client, Schema.BYTES));
        final CompletableFuture<Consumer<byte[]>> consumerFuture = new CompletableFuture<>();
        final Consumer<byte[]> consumer = mock(Consumer.class);
        final CompletableFuture<Void> ackFuture = new CompletableFuture<>();
        final CompletableFuture<Void> closeFuture = new CompletableFuture<>();
        final MessageMetadata metadata = new MessageMetadata();
        metadata.setPublishTime(System.currentTimeMillis());
        final List<CompletableFuture<Message<byte[]>>> msgList = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            final CompletableFuture<Message<byte[]>> msgFuture = new CompletableFuture<>();
            msgFuture.complete(new MessageImpl<byte[]>(topicName.toString(), "0:" + i,
                    Collections.emptyMap(), new byte[0], Schema.BYTES, metadata));
            msgList.add(msgFuture);
        }
        final ConsumerCommand ackCmd = new ConsumerCommand();
        ackCmd.messageId
                = Base64.getEncoder().encodeToString(msgList.get(0).get().getMessageId().toByteArray());

        closeFuture.complete(null);
        when(consumer.getTopic()).thenReturn(topicName.toString());
        when(consumer.receiveAsync())
                .thenReturn(msgList.get(0), msgList.get(1), msgList.get(2), msgList.get(3), msgList.get(4));
        when(consumer.acknowledgeAsync(any(MessageId.class))).thenReturn(ackFuture);
        doNothing().when(consumer).negativeAcknowledge(any(MessageId.class));
        when(consumer.closeAsync()).thenReturn(closeFuture);
        consumerFuture.complete(consumer);
        when(builder.subscribeAsync()).thenReturn(consumerFuture);
        when(client.newConsumer()).thenReturn(builder);
        when(session.getRemote()).thenReturn(remoteEndpoint);
        when(service.getConfig()).thenReturn(config);
        when(service.getPulsarClient()).thenReturn(client);
        when(service.addConsumer(any(ConsumerHandler.class))).thenReturn(true);
        when(service.getExecutor()).thenReturn(Executors
                .newScheduledThreadPool(config.getWebSocketNumServiceThreads(),
                        new DefaultThreadFactory("pulsar-websocket")));

        final ConsumerHandler handler = new ConsumerHandler(service, httpServletRequest, response);
        handler.onWebSocketConnect(session);
        final Field blockedField = ConsumerHandler.class.getDeclaredField("blocked");
        blockedField.setAccessible(true);
        final AtomicBoolean blocked = (AtomicBoolean) blockedField.get(handler);
        final Field pendingMessagesField = ConsumerHandler.class.getDeclaredField("pendingMessages");
        pendingMessagesField.setAccessible(true);
        final Set<String> pendingMessages
                = ((Optional<Set<String>>) pendingMessagesField.get(handler)).get();
        Awaitility.await().untilAsserted(() -> assertTrue(blocked.get()));
        assertTrue(pendingMessages.contains(ackCmd.messageId));
        assertTrue(pendingMessages.size() >= 2);
        assertTrue(pendingMessages.size() < 5);

        // acknowledge
        handler.onWebSocketText(ObjectMapperFactory.getThreadLocal().writeValueAsString(ackCmd));
        Awaitility.await()
                .untilAsserted(() -> assertFalse(pendingMessages.contains(ackCmd.messageId)));
        assertTrue(pendingMessages.size() >= 2);
        assertTrue(pendingMessages.size() < 5);

        final Set<String> pendingMessagesSnapshot = new HashSet<>(pendingMessages);

        // acknowledge by not received message id
        final ConsumerCommand ackByNotReceivedCmd = new ConsumerCommand();
        ackByNotReceivedCmd.messageId
                = Base64.getEncoder().encodeToString((new MessageIdImpl(1, 1, -1)).toByteArray());
        handler.onWebSocketText(ObjectMapperFactory.getThreadLocal().writeValueAsString(ackByNotReceivedCmd));
        Awaitility.await()
                .untilAsserted(() -> assertNotEquals(pendingMessages, pendingMessagesSnapshot));
        assertTrue(pendingMessages.size() > pendingMessagesSnapshot.size());
        assertTrue(pendingMessages.containsAll(pendingMessagesSnapshot));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void pullPermitsTest() throws Exception {
        final TopicName topicName = TopicName.get("persistent://my-tenant/my-ns/pull-permits-test");
        final String consumeV2 = "/ws/v2/consumer/" + topicName.getRestPath() + "/my-sub";
        final Map<String, String[]> queryParams = new HashMap<String, String>(){{
            put("pullMode", "true");
        }}.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, entry -> new String[]{ entry.getValue() }));

        final HttpServletRequest httpServletRequest = mock(HttpServletRequest.class);
        when(httpServletRequest.getRequestURI()).thenReturn(consumeV2);
        when(httpServletRequest.getParameterMap()).thenReturn(queryParams);

        final WebSocketService service = mock(WebSocketService.class);
        final ServiceConfiguration config = PulsarConfigurationLoader.convertFrom(
                PulsarConfigurationLoader.create(
                        this.getClass().getClassLoader().getResource("websocket.conf").getFile(),
                        WebSocketProxyConfiguration.class));
        config.setAuthenticationEnabled(false);
        config.setAuthorizationEnabled(false);
        final Session session = mock(Session.class);
        final ServletUpgradeResponse response = mock(ServletUpgradeResponse.class);
        final RemoteEndpoint remoteEndpoint = mock(RemoteEndpoint.class);
        final PulsarClientImpl client = mock(PulsarClientImpl.class);
        final ConsumerBuilder<byte[]> builder = spy(new ConsumerBuilderImpl<>(client, Schema.BYTES));
        final CompletableFuture<Consumer<byte[]>> consumerFuture = new CompletableFuture<>();
        final Consumer<byte[]> consumer = mock(Consumer.class);
        final CompletableFuture<Void> ackFuture = new CompletableFuture<>();
        final CompletableFuture<Void> closeFuture = new CompletableFuture<>();
        final MessageMetadata metadata = new MessageMetadata();
        metadata.setPublishTime(System.currentTimeMillis());
        final List<CompletableFuture<Message<byte[]>>> msgList = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            final CompletableFuture<Message<byte[]>> msgFuture = new CompletableFuture<>();
            msgFuture.complete(new MessageImpl<byte[]>(topicName.toString(), "0:" + i,
                    Collections.emptyMap(), new byte[0], Schema.BYTES, metadata));
            msgList.add(msgFuture);
        }
        final ConsumerCommand ackCmd = new ConsumerCommand();
        ackCmd.messageId
                = Base64.getEncoder().encodeToString(msgList.get(0).get().getMessageId().toByteArray());

        final Set<ConsumerMessage> receivedMsgSet = new ConcurrentHashSet<>();

        closeFuture.complete(null);
        when(consumer.getTopic()).thenReturn(topicName.toString());
        when(consumer.receiveAsync())
                .thenReturn(msgList.get(0), msgList.get(1), msgList.get(2), msgList.get(3), msgList.get(4));
        when(consumer.acknowledgeAsync(any(MessageId.class))).thenReturn(ackFuture);
        doNothing().when(consumer).negativeAcknowledge(any(MessageId.class));
        when(consumer.closeAsync()).thenReturn(closeFuture);
        consumerFuture.complete(consumer);
        when(builder.subscribeAsync()).thenReturn(consumerFuture);
        when(client.newConsumer()).thenReturn(builder);
        doAnswer(invocationOnMock -> receivedMsgSet.add(ObjectMapperFactory.getThreadLocal()
                .readValue((String) invocationOnMock.getArgument(0), ConsumerMessage.class)))
                .when(remoteEndpoint).sendString(anyString(), any(WriteCallback.class));
        when(session.getRemote()).thenReturn(remoteEndpoint);
        when(service.getConfig()).thenReturn(config);
        when(service.getPulsarClient()).thenReturn(client);
        when(service.addConsumer(any(ConsumerHandler.class))).thenReturn(true);
        when(service.getExecutor()).thenReturn(Executors
                .newScheduledThreadPool(config.getWebSocketNumServiceThreads(),
                        new DefaultThreadFactory("pulsar-websocket")));

        final ConsumerHandler handler = new ConsumerHandler(service, httpServletRequest, response);
        handler.onWebSocketConnect(session);
        final Field permitsField = ConsumerHandler.class.getDeclaredField("permits");
        permitsField.setAccessible(true);
        final AtomicInteger permits = (AtomicInteger) permitsField.get(handler);
        assertEquals(permits.get(), 0);

        // permit
        final ConsumerCommand permitCmd = new ConsumerCommand();
        permitCmd.type = "permit";
        permitCmd.permitMessages = 2;
        handler.onWebSocketText(ObjectMapperFactory.getThreadLocal().writeValueAsString(permitCmd));
        Awaitility.await().untilAsserted(() -> assertEquals(permits.get(), 0));
        final int receivedMsgSetSize = receivedMsgSet.size();
        assertTrue(receivedMsgSetSize >= 2);
        assertTrue(receivedMsgSetSize < 5);

        // acknowledge
        handler.onWebSocketText(ObjectMapperFactory.getThreadLocal().writeValueAsString(ackCmd));
        assertEquals(receivedMsgSet.size(), receivedMsgSetSize);

        // permit
        handler.onWebSocketText(ObjectMapperFactory.getThreadLocal().writeValueAsString(permitCmd));
        Awaitility.await().untilAsserted(() -> assertEquals(permits.get(), 0));
        assertTrue(receivedMsgSet.size() >= 4);
    }
}
