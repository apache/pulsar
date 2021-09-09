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

import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.websocket.data.ProducerMessage;
import org.eclipse.jetty.websocket.servlet.ServletUpgradeResponse;
import org.testng.annotations.Test;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ProducerHandlerTest {

    @Test
    public void testProduceMessageAttributes() throws IOException {
        String producerV2 = "/ws/v2/producer/persistent/my-property/my-ns/my-topic";
        HttpServletRequest httpServletRequest = mock(HttpServletRequest.class);
        PulsarClient pulsarClient = mock(PulsarClient.class);
        ProducerBuilder producerBuilder = mock(ProducerBuilder.class);
        Producer producer = mock(Producer.class);
        TypedMessageBuilder messageBuilder = mock(TypedMessageBuilder.class);
        ProducerMessage produceRequest = new ProducerMessage();

        produceRequest.setDeliverAfterMs(11111);
        produceRequest.setDeliverAt(22222);
        produceRequest.setContext("context");
        produceRequest.setPayload(Base64.getEncoder().encodeToString("my payload".getBytes()));

        // the params are all different with the default value
        Map<String, String[]> queryParams = new HashMap<>();

        httpServletRequest = mock(HttpServletRequest.class);
        when(httpServletRequest.getRequestURI()).thenReturn(producerV2);
        when(httpServletRequest.getParameterMap()).thenReturn(queryParams);

        WebSocketService service = mock(WebSocketService.class);
        when(service.isAuthenticationEnabled()).thenReturn(false);
        when(service.isAuthorizationEnabled()).thenReturn(false);
        when(service.getPulsarClient()).thenReturn(pulsarClient);

        when(pulsarClient.newProducer()).thenReturn(producerBuilder);
        when(producerBuilder.enableBatching(anyBoolean())).thenReturn(producerBuilder);
        when(producerBuilder.messageRoutingMode(any())).thenReturn(producerBuilder);
        when(producerBuilder.blockIfQueueFull(anyBoolean())).thenReturn(producerBuilder);
        when(producerBuilder.topic(anyString())).thenReturn(producerBuilder);
        when(producerBuilder.create()).thenReturn(producer);

        when(producer.newMessage()).thenReturn(messageBuilder);
        when(messageBuilder.sendAsync()).thenReturn( CompletableFuture.completedFuture(new MessageIdImpl(1, 2, 3)));

        ServletUpgradeResponse response = mock(ServletUpgradeResponse.class);

        ProducerHandler producerHandler = new ProducerHandler(service, httpServletRequest, response);
        producerHandler.onWebSocketText(ObjectMapperFactory.getThreadLocal().writeValueAsString(produceRequest));

        verify(messageBuilder, times(1)).deliverAfter(11111, TimeUnit.MILLISECONDS);
        verify(messageBuilder, times(1)).deliverAt(22222);
    }

}
