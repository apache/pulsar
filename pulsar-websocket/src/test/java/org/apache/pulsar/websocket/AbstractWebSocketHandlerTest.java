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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import lombok.Getter;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.HashingScheme;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.ConsumerBuilderImpl;
import org.apache.pulsar.client.impl.ProducerBuilderImpl;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.apache.pulsar.common.naming.TopicName;
import org.eclipse.jetty.websocket.servlet.ServletUpgradeResponse;
import org.mockito.Mock;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

public class AbstractWebSocketHandlerTest {
    List<PulsarClient> createdClients = Collections.synchronizedList(new ArrayList<>());

    @Mock
    private HttpServletRequest httpServletRequest;

    @AfterClass(alwaysRun = true)
    final void cleanupCreatedClients() {
        for (PulsarClient createdClient : createdClients) {
            try {
                createdClient.shutdown();
            } catch (PulsarClientException e) {
                // ignore
            }
        }
        createdClients.clear();
    }

    @Test
    public void topicNameUrlEncodingTest() throws Exception {
        String producerV1 = "/ws/producer/persistent/my-property/my-cluster/my-ns/";
        String producerV1Topic = "my-topic[]<>";
        String consumerV1 = "/ws/consumer/persistent/my-property/my-cluster/my-ns/";
        String consumerV1Topic = "my-topic!@#!@@!#";
        String consumerV1Sub = "my-subscription[]<>!@#$%^&*( )";

        String readerV1 = "/ws/reader/persistent/my-property/my-cluster/my-ns/";
        String readerV1Topic = "my-topic[]!) (*&^%$#@";

        String producerV2 = "/ws/v2/producer/persistent/my-property/my-ns/";
        String producerV2Topic = "my-topic[]<>";
        String consumerV2 = "/ws/v2/consumer/persistent/my-property/my-ns/";
        String consumerV2Topic = "my-topic";
        String consumerV2Sub = "my-subscription[][]<>";
        String readerV2 = "/ws/v2/reader/persistent/my-property/my-ns/";
        String readerV2Topic = "my-topic/ / /@!$#^&*( /)1 /_、`，《》</>[]";

        httpServletRequest = mock(HttpServletRequest.class);

        when(httpServletRequest.getRequestURI()).thenReturn(producerV1 + URLEncoder.encode(producerV1Topic, StandardCharsets.UTF_8.name()));
        WebSocketHandlerImpl webSocketHandler = new WebSocketHandlerImpl(null, httpServletRequest, null);
        TopicName topicName = webSocketHandler.getTopic();
        assertEquals(topicName.toString(), "persistent://my-property/my-cluster/my-ns/" + producerV1Topic);

        when(httpServletRequest.getRequestURI()).thenReturn(consumerV1
                + URLEncoder.encode(consumerV1Topic, StandardCharsets.UTF_8.name()) + "/"
                + URLEncoder.encode(consumerV1Sub, StandardCharsets.UTF_8.name()));
        webSocketHandler = new WebSocketHandlerImpl(null, httpServletRequest, null);
        topicName = webSocketHandler.getTopic();
        assertEquals(topicName.toString(), "persistent://my-property/my-cluster/my-ns/" + consumerV1Topic);

        when(httpServletRequest.getRequestURI()).thenReturn(readerV1
                + URLEncoder.encode(readerV1Topic, StandardCharsets.UTF_8.name()));
        webSocketHandler = new WebSocketHandlerImpl(null, httpServletRequest, null);
        topicName = webSocketHandler.getTopic();
        assertEquals(topicName.toString(), "persistent://my-property/my-cluster/my-ns/" + readerV1Topic);

        when(httpServletRequest.getRequestURI()).thenReturn(producerV2
                + URLEncoder.encode(producerV2Topic, StandardCharsets.UTF_8.name()));
        webSocketHandler = new WebSocketHandlerImpl(null, httpServletRequest, null);
        topicName = webSocketHandler.getTopic();
        assertEquals(topicName.toString(), "persistent://my-property/my-ns/" + producerV2Topic);

        when(httpServletRequest.getRequestURI()).thenReturn(consumerV2
                + URLEncoder.encode(consumerV2Topic, StandardCharsets.UTF_8.name()) + "/"
                + URLEncoder.encode(consumerV2Sub, StandardCharsets.UTF_8.name()));
        webSocketHandler = new WebSocketHandlerImpl(null, httpServletRequest, null);
        topicName = webSocketHandler.getTopic();
        assertEquals(topicName.toString(), "persistent://my-property/my-ns/" + consumerV2Topic);
        String sub = ConsumerHandler.extractSubscription(httpServletRequest);
        assertEquals(sub, consumerV2Sub);

        when(httpServletRequest.getRequestURI()).thenReturn(readerV2
                + URLEncoder.encode(readerV2Topic, StandardCharsets.UTF_8.name()));
        webSocketHandler = new WebSocketHandlerImpl(null, httpServletRequest, null);
        topicName = webSocketHandler.getTopic();
        assertEquals(topicName.toString(), "persistent://my-property/my-ns/" + readerV2Topic);
    }

    @Test
    public void parseTopicNameTest() {
        String producerV1 = "/ws/producer/persistent/my-property/my-cluster/my-ns/my-topic";
        String consumerV1 = "/ws/consumer/persistent/my-property/my-cluster/my-ns/my-topic/my-subscription";
        String readerV1 = "/ws/reader/persistent/my-property/my-cluster/my-ns/my-topic";

        String producerV2 = "/ws/v2/producer/persistent/my-property/my-ns/my-topic";
        String consumerV2 = "/ws/v2/consumer/persistent/my-property/my-ns/my-topic/my-subscription";
        String consumerLongTopicNameV2 = "/ws/v2/consumer/persistent/my-tenant/my-ns/some/topic/with/slashes/my-sub";
        String readerV2 = "/ws/v2/reader/persistent/my-property/my-ns/my-topic/ / /@!$#^&*( /)1 /_、`，《》</>";

        httpServletRequest = mock(HttpServletRequest.class);

        when(httpServletRequest.getRequestURI()).thenReturn(producerV1);
        WebSocketHandlerImpl webSocketHandler = new WebSocketHandlerImpl(null, httpServletRequest, null);
        TopicName topicName = webSocketHandler.getTopic();
        assertEquals(topicName.toString(), "persistent://my-property/my-cluster/my-ns/my-topic");

        when(httpServletRequest.getRequestURI()).thenReturn(consumerV1);
        webSocketHandler = new WebSocketHandlerImpl(null, httpServletRequest, null);
        topicName = webSocketHandler.getTopic();
        assertEquals(topicName.toString(), "persistent://my-property/my-cluster/my-ns/my-topic");

        when(httpServletRequest.getRequestURI()).thenReturn(readerV1);
        webSocketHandler = new WebSocketHandlerImpl(null, httpServletRequest, null);
        topicName = webSocketHandler.getTopic();
        assertEquals(topicName.toString(), "persistent://my-property/my-cluster/my-ns/my-topic");

        when(httpServletRequest.getRequestURI()).thenReturn(producerV2);
        webSocketHandler = new WebSocketHandlerImpl(null, httpServletRequest, null);
        topicName = webSocketHandler.getTopic();
        assertEquals(topicName.toString(), "persistent://my-property/my-ns/my-topic");

        when(httpServletRequest.getRequestURI()).thenReturn(consumerV2);
        webSocketHandler = new WebSocketHandlerImpl(null, httpServletRequest, null);
        topicName = webSocketHandler.getTopic();
        assertEquals(topicName.toString(), "persistent://my-property/my-ns/my-topic");

        when(httpServletRequest.getRequestURI()).thenReturn(consumerLongTopicNameV2);
        webSocketHandler = new WebSocketHandlerImpl(null, httpServletRequest, null);
        topicName = webSocketHandler.getTopic();
        assertEquals(topicName.toString(), "persistent://my-tenant/my-ns/some/topic/with/slashes");

        when(httpServletRequest.getRequestURI()).thenReturn(readerV2);
        webSocketHandler = new WebSocketHandlerImpl(null, httpServletRequest, null);
        topicName = webSocketHandler.getTopic();
        assertEquals(topicName.toString(), "persistent://my-property/my-ns/my-topic/ / /@!$#^&*( /)1 /_、`，《》</>");

    }

    static class WebSocketHandlerImpl extends AbstractWebSocketHandler {

        public WebSocketHandlerImpl(WebSocketService service, HttpServletRequest request, ServletUpgradeResponse response) {
            super(service, request, response);
        }

        @Override
        protected Boolean isAuthorized(String authRole, AuthenticationDataSource authenticationData) {
            return null;
        }

        @Override
        public void close() throws IOException {

        }

        public TopicName getTopic() {
            return super.topic;
        }

    }

    static class MockedServletUpgradeResponse extends ServletUpgradeResponse {

        @Getter
        private int statusCode;
        @Getter
        private String message;

        public MockedServletUpgradeResponse(HttpServletResponse response) {
            super(response);
        }

        public void sendError(int statusCode, String message) {
            this.statusCode = statusCode;
            this.message = message;
        }
    }

    PulsarClient newPulsarClient() throws PulsarClientException {
        PulsarClient client = PulsarClient.builder()
                .serviceUrl("pulsar://localhost:6650")
                .operationTimeout(1, TimeUnit.SECONDS)
                .build();
        createdClients.add(client);
        return client;
    }

    class MockedProducerHandler extends ProducerHandler {

        public MockedProducerHandler(WebSocketService service, HttpServletRequest request, ServletUpgradeResponse response) {
            super(service, request, response);
        }

        public ProducerConfigurationData getConf() throws PulsarClientException {
            return ((ProducerBuilderImpl<byte[]>) getProducerBuilder(newPulsarClient())).getConf();
        }

        public void clearQueryParams() {
            queryParams.clear();
        }

        public void putQueryParam(String key, String value) {
            queryParams.put(key, value);
        }
    }

    @Test
    public void producerBuilderTest() throws IOException {
        String producerV2 = "/ws/v2/producer/persistent/my-property/my-ns/my-topic";
        // the params are all different with the default value
        Map<String, String[]> queryParams = new HashMap<String, String>(){{
            put("producerName", "my-producer");
            put("initialSequenceId", "1");
            put("hashingScheme", "Murmur3_32Hash");
            put("sendTimeoutMillis", "30001");
            put("batchingEnabled", "false");
            put("batchingMaxMessages", "1001");
            put("maxPendingMessages", "1001");
            put("batchingMaxPublishDelay", "2");
            put("messageRoutingMode", "RoundRobinPartition");
            put("compressionType", "LZ4");
        }}.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, entry -> new String[]{ entry.getValue() }));

        httpServletRequest = mock(HttpServletRequest.class);
        when(httpServletRequest.getRequestURI()).thenReturn(producerV2);
        when(httpServletRequest.getParameterMap()).thenReturn(queryParams);

        WebSocketService service = mock(WebSocketService.class);
        when(service.isAuthenticationEnabled()).thenReturn(false);
        when(service.isAuthorizationEnabled()).thenReturn(false);
        when(service.getPulsarClient()).thenReturn(newPulsarClient());

        MockedServletUpgradeResponse response = new MockedServletUpgradeResponse(null);

        MockedProducerHandler producerHandler = new MockedProducerHandler(service, httpServletRequest, response);
        assertEquals(response.getStatusCode(), 500);
        assertTrue(response.getMessage().contains("Connection refused"));

        ProducerConfigurationData conf = producerHandler.getConf();
        assertEquals(conf.getProducerName(), "my-producer");
        assertEquals(conf.getInitialSequenceId().longValue(), 1L);
        assertEquals(conf.getHashingScheme(), HashingScheme.Murmur3_32Hash);
        assertEquals(conf.getSendTimeoutMs(), 30001);
        assertFalse(conf.isBatchingEnabled() );
        assertEquals(conf.getBatchingMaxMessages(), 1001);
        assertEquals(conf.getMaxPendingMessages(), 1001);
        assertEquals(conf.getMessageRoutingMode(), MessageRoutingMode.RoundRobinPartition);
        assertEquals(conf.getCompressionType(), CompressionType.LZ4);

        producerHandler.clearQueryParams();
        conf = producerHandler.getConf();
        // The default message routing mode is SinglePartition, which is different with ProducerBuilder
        assertEquals(conf.getMessageRoutingMode(), MessageRoutingMode.SinglePartition);

        producerHandler.putQueryParam("messageRoutingMode", "CustomPartition");
        conf = producerHandler.getConf();
        // ProducerHandler doesn't support CustomPartition
        assertEquals(conf.getMessageRoutingMode(), MessageRoutingMode.SinglePartition);
    }

    class MockedConsumerHandler extends ConsumerHandler {

        public MockedConsumerHandler(WebSocketService service, HttpServletRequest request, ServletUpgradeResponse response) {
            super(service, request, response);
        }

        public ConsumerConfigurationData<byte[]> getConf() throws PulsarClientException {
            return ((ConsumerBuilderImpl<byte[]>) getConsumerConfiguration(newPulsarClient())).getConf();
        }

        public void clearQueryParams() {
            queryParams.clear();
        }

        public void putQueryParam(String key, String value) {
            queryParams.put(key, value);
        }
    }

    @Test
    public void consumerBuilderTest() throws IOException {
        String consumerV2 = "/ws/v2/consumer/persistent/my-property/my-ns/my-topic/my-subscription";
        // the params are all different with the default value
        Map<String, String[]> queryParams = new HashMap<String, String>(){{
            put("ackTimeoutMillis", "1001");
            put("subscriptionType", "Key_Shared");
            put("subscriptionMode", "NonDurable");
            put("receiverQueueSize", "999");
            put("consumerName", "my-consumer");
            put("priorityLevel", "1");
            put("maxRedeliverCount", "5");
        }}.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, entry -> new String[]{ entry.getValue() }));

        httpServletRequest = mock(HttpServletRequest.class);
        when(httpServletRequest.getRequestURI()).thenReturn(consumerV2);
        when(httpServletRequest.getParameterMap()).thenReturn(queryParams);

        WebSocketService service = mock(WebSocketService.class);
        when(service.isAuthenticationEnabled()).thenReturn(false);
        when(service.isAuthorizationEnabled()).thenReturn(false);
        when(service.getPulsarClient()).thenReturn(newPulsarClient());

        MockedServletUpgradeResponse response = new MockedServletUpgradeResponse(null);

        MockedConsumerHandler consumerHandler = new MockedConsumerHandler(service, httpServletRequest, response);
        assertEquals(response.getStatusCode(), 500);
        assertTrue(response.getMessage().contains("Connection refused"));
        assertEquals(consumerHandler.getSubscriptionMode(), SubscriptionMode.NonDurable);
        assertEquals(consumerHandler.getSubscriptionType(), SubscriptionType.Key_Shared);

        ConsumerConfigurationData<byte[]> conf = consumerHandler.getConf();
        assertEquals(conf.getAckTimeoutMillis(), 1001);
        assertEquals(conf.getSubscriptionType(), SubscriptionType.Key_Shared);
        assertEquals(conf.getSubscriptionMode(), SubscriptionMode.NonDurable);
        assertEquals(conf.getReceiverQueueSize(), 999);
        assertEquals(conf.getConsumerName(), "my-consumer");
        assertEquals(conf.getPriorityLevel(), 1);
        assertEquals(conf.getDeadLetterPolicy().getDeadLetterTopic(),
                "persistent://my-property/my-ns/my-topic-my-subscription-DLQ");
        assertEquals(conf.getDeadLetterPolicy().getMaxRedeliverCount(), 5);

        consumerHandler.clearQueryParams();
        consumerHandler.putQueryParam("receiverQueueSize", "1001");
        consumerHandler.putQueryParam("deadLetterTopic", "dead-letter-topic");

        conf = consumerHandler.getConf();
        // receive queue size is the minimum value of default value (1000) and user defined value(1001)
        assertEquals(conf.getReceiverQueueSize(), 1000);
        assertEquals(conf.getDeadLetterPolicy().getDeadLetterTopic(), "dead-letter-topic");
        assertEquals(conf.getDeadLetterPolicy().getMaxRedeliverCount(), 0);
    }
}
