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

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.ReaderBuilder;
import org.apache.pulsar.client.impl.ConsumerImpl;
import org.apache.pulsar.client.impl.MultiTopicsConsumerImpl;
import org.apache.pulsar.client.impl.MultiTopicsReaderImpl;
import org.apache.pulsar.client.impl.ReaderImpl;
import org.eclipse.jetty.websocket.servlet.ServletUpgradeResponse;
import org.testng.Assert;
import org.testng.annotations.Test;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ReaderHandlerTest {

    @Test
    @SuppressWarnings("unchecked")
    public void testCreateReaderImp() throws IOException {
        final String subName = "readerImpSubscription";
        // mock data
        WebSocketService wss = mock(WebSocketService.class);
        PulsarClient mockedClient = mock(PulsarClient.class);
        when(wss.getPulsarClient()).thenReturn(mockedClient);
        ReaderBuilder<byte[]> mockedReaderBuilder = mock(ReaderBuilder.class);
        when(mockedClient.newReader()).thenReturn(mockedReaderBuilder);
        when(mockedReaderBuilder.topic(any())).thenReturn(mockedReaderBuilder);
        when(mockedReaderBuilder.startMessageId(any())).thenReturn(mockedReaderBuilder);
        when(mockedReaderBuilder.receiverQueueSize(anyInt())).thenReturn(mockedReaderBuilder);
        ReaderImpl<byte[]> mockedReader = mock(ReaderImpl.class);
        when(mockedReaderBuilder.create()).thenReturn(mockedReader);
        ConsumerImpl<byte[]> consumerImp = mock(ConsumerImpl.class);
        when(consumerImp.getSubscription()).thenReturn(subName);
        when(mockedReader.getConsumer()).thenReturn(consumerImp);
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getRequestURI()).thenReturn("/ws/v2/producer/persistent/my-property/my-ns/my-topic");
        // create reader handler
        HttpServletResponse response = spy(HttpServletResponse.class);
        ServletUpgradeResponse servletUpgradeResponse = new ServletUpgradeResponse(response);
        ReaderHandler readerHandler = new ReaderHandler(wss, request, servletUpgradeResponse);
        // verify success
        Assert.assertEquals(readerHandler.getSubscription(), subName);
        // Verify consumer is returned
        readerHandler.getConsumer();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testCreateMultipleTopicReaderImp() throws IOException {
        final String subName = "multipleTopicReaderImpSubscription";
        // mock data
        WebSocketService wss = mock(WebSocketService.class);
        PulsarClient mockedClient = mock(PulsarClient.class);
        when(wss.getPulsarClient()).thenReturn(mockedClient);
        ReaderBuilder<byte[]> mockedReaderBuilder = mock(ReaderBuilder.class);
        when(mockedClient.newReader()).thenReturn(mockedReaderBuilder);
        when(mockedReaderBuilder.topic(any())).thenReturn(mockedReaderBuilder);
        when(mockedReaderBuilder.startMessageId(any())).thenReturn(mockedReaderBuilder);
        when(mockedReaderBuilder.receiverQueueSize(anyInt())).thenReturn(mockedReaderBuilder);
        MultiTopicsReaderImpl<byte[]> mockedReader = mock(MultiTopicsReaderImpl.class);
        when(mockedReaderBuilder.create()).thenReturn(mockedReader);
        MultiTopicsConsumerImpl<byte[]> consumerImp = mock(MultiTopicsConsumerImpl.class);
        when(consumerImp.getSubscription()).thenReturn(subName);
        when(mockedReader.getMultiTopicsConsumer()).thenReturn(consumerImp);
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getRequestURI()).thenReturn("/ws/v2/producer/persistent/my-property/my-ns/my-topic");
        // create reader handler
        HttpServletResponse response = spy(HttpServletResponse.class);
        ServletUpgradeResponse servletUpgradeResponse = new ServletUpgradeResponse(response);
        ReaderHandler readerHandler = new ReaderHandler(wss, request, servletUpgradeResponse);
        // verify success
        Assert.assertEquals(readerHandler.getSubscription(), subName);
        // Verify consumer is successfully returned
        readerHandler.getConsumer();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testCreateIllegalReaderImp() throws IOException {
        // mock data
        WebSocketService wss = mock(WebSocketService.class);
        PulsarClient mockedClient = mock(PulsarClient.class);
        when(wss.getPulsarClient()).thenReturn(mockedClient);
        ReaderBuilder<byte[]> mockedReaderBuilder = mock(ReaderBuilder.class);
        when(mockedClient.newReader()).thenReturn(mockedReaderBuilder);
        when(mockedReaderBuilder.topic(any())).thenReturn(mockedReaderBuilder);
        when(mockedReaderBuilder.startMessageId(any())).thenReturn(mockedReaderBuilder);
        when(mockedReaderBuilder.receiverQueueSize(anyInt())).thenReturn(mockedReaderBuilder);
        IllegalReader illegalReader = new IllegalReader();
        when(mockedReaderBuilder.create()).thenReturn(illegalReader);
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getRequestURI()).thenReturn("/ws/v2/producer/persistent/my-property/my-ns/my-topic");
        // create reader handler
        HttpServletResponse response = spy(HttpServletResponse.class);
        ServletUpgradeResponse servletUpgradeResponse = new ServletUpgradeResponse(response);
        new ReaderHandler(wss, request, servletUpgradeResponse);
        // verify get error
        verify(response, times(1)).sendError(anyInt(), anyString());
    }


    static class IllegalReader implements Reader<byte[]> {

        @Override
        public String getTopic() {
            return null;
        }

        @Override
        public Message<byte[]> readNext() throws PulsarClientException {
            return null;
        }

        @Override
        public Message<byte[]> readNext(int timeout, TimeUnit unit) throws PulsarClientException {
            return null;
        }

        @Override
        public CompletableFuture<Message<byte[]>> readNextAsync() {
            return null;
        }

        @Override
        public CompletableFuture<Void> closeAsync() {
            return null;
        }

        @Override
        public boolean hasReachedEndOfTopic() {
            return false;
        }

        @Override
        public boolean hasMessageAvailable() {
            return false;
        }

        @Override
        public CompletableFuture<Boolean> hasMessageAvailableAsync() {
            return null;
        }

        @Override
        public boolean isConnected() {
            return false;
        }

        @Override
        public void seek(MessageId messageId) throws PulsarClientException {

        }

        @Override
        public void seek(long timestamp) throws PulsarClientException {

        }

        @Override
        public void seek(Function<String, Object> function) throws PulsarClientException {

        }

        @Override
        public CompletableFuture<Void> seekAsync(Function<String, Object> function) {
            return null;
        }

        @Override
        public CompletableFuture<Void> seekAsync(MessageId messageId) {
            return null;
        }

        @Override
        public CompletableFuture<Void> seekAsync(long timestamp) {
            return null;
        }

        @Override
        public void close() throws IOException {

        }
    }
}
