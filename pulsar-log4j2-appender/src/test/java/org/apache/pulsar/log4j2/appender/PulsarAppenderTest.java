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
package org.apache.pulsar.log4j2.appender;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.impl.Log4jLogEvent;
import org.apache.logging.log4j.message.SimpleMessage;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.ClientBuilderImpl;
import org.apache.pulsar.client.impl.TypedMessageBuilderImpl;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class PulsarAppenderTest {

    private static final String LOG_MESSAGE = "Hello, world!";

    private static Log4jLogEvent createLogEvent() {
        return Log4jLogEvent.newBuilder()
            .setLoggerName(PulsarAppenderTest.class.getName())
            .setLoggerFqcn(PulsarAppenderTest.class.getName())
            .setLevel(Level.INFO)
            .setMessage(new SimpleMessage(LOG_MESSAGE))
            .build();
    }

    private ClientBuilderImpl clientBuilder;
    private PulsarClient client;
    private Producer<byte[]> producer;
    private List<Message<byte[]>> history;

    private LoggerContext ctx;

    private class MockedMessageBuilder extends TypedMessageBuilderImpl<byte[]> {

        MockedMessageBuilder() {
            super(null, Schema.BYTES);
        }

        @Override
        public MessageId send() {
            synchronized (history) {
                history.add(getMessage());
            }

            return mock(MessageId.class);
        }

        @Override
        public CompletableFuture<MessageId> sendAsync() {
            synchronized (history) {
                history.add(getMessage());
            }

            return CompletableFuture.completedFuture(mock(MessageId.class));
        }
    }

    @BeforeMethod
    public void setUp() throws Exception {
        history = new LinkedList<>();

        client = mock(PulsarClient.class);
        producer = mock(Producer.class);
        clientBuilder = mock(ClientBuilderImpl.class);

        doReturn(client).when(clientBuilder).build();
        doReturn(clientBuilder).when(clientBuilder).serviceUrl(anyString());

        ProducerBuilder<byte[]> producerBuilder = mock(ProducerBuilder.class);
        when(client.newProducer()).thenReturn(producerBuilder);
        doReturn(producerBuilder).when(producerBuilder).topic(anyString());
        doReturn(producerBuilder).when(producerBuilder).producerName(anyString());
        doReturn(producerBuilder).when(producerBuilder).enableBatching(anyBoolean());
        doReturn(producerBuilder).when(producerBuilder).batchingMaxPublishDelay(anyLong(), any(TimeUnit.class));
        doReturn(producerBuilder).when(producerBuilder).blockIfQueueFull(anyBoolean());
        doReturn(producer).when(producerBuilder).create();

        when(producer.newMessage()).then(invocation -> new MockedMessageBuilder());
        when(producer.send(any(byte[].class)))
            .thenAnswer(invocationOnMock -> {
                Message<byte[]> msg = invocationOnMock.getArgument(0);
                synchronized (history) {
                    history.add(msg);
                }
                return null;
            });

        when(producer.sendAsync(any(byte[].class)))
            .thenAnswer(invocationOnMock -> {
                Message<byte[]> msg = invocationOnMock.getArgument(0);
                synchronized (history) {
                    history.add(msg);
                }
                CompletableFuture<MessageId> future = new CompletableFuture<>();
                future.complete(mock(MessageId.class));
                return future;
            });

        PulsarManager.PULSAR_CLIENT_BUILDER = () -> clientBuilder;

        ctx = Configurator.initialize(
            "PulsarAppenderTest",
            getClass().getClassLoader(),
            getClass().getClassLoader().getResource("PulsarAppenderTest.xml").toURI());
    }

    @Test
    public void testAppendWithLayout() throws Exception {
        final Appender appender = ctx.getConfiguration().getAppender("PulsarAppenderWithLayout");
        appender.append(createLogEvent());
        final Message<byte[]> item;
        synchronized (history) {
            assertEquals(1, history.size());
            item = history.get(0);
        }
        assertNotNull(item);
        assertFalse(item.hasKey());
        assertEquals("[" + LOG_MESSAGE + "]", new String(item.getData(), StandardCharsets.UTF_8));
    }

    @Test
    public void testAppendWithSerializedLayout() throws Exception {
        final Appender appender = ctx.getConfiguration().getAppender("PulsarAppenderWithSerializedLayout");
        final LogEvent logEvent = createLogEvent();
        appender.append(logEvent);
        final Message<byte[]> item;
        synchronized (history) {
            assertEquals(1, history.size());
            item = history.get(0);
        }
        assertNotNull(item);
        assertFalse(item.hasKey());
        assertEquals(LOG_MESSAGE, deserializeLogEvent(item.getData()).getMessage().getFormattedMessage());
    }

    @Test
    public void testAsyncAppend() {
        final Appender appender = ctx.getConfiguration().getAppender("AsyncPulsarAppender");
        appender.append(createLogEvent());
        final Message<byte[]> item;
        synchronized (history) {
            assertEquals(1, history.size());
            item = history.get(0);
        }
        assertNotNull(item);
        assertFalse(item.hasKey());
        assertEquals(LOG_MESSAGE, new String(item.getData(), StandardCharsets.UTF_8));
    }

    @Test
    public void testAppendWithKey() {
        final Appender appender = ctx.getConfiguration().getAppender("PulsarAppenderWithKey");
        final LogEvent logEvent = createLogEvent();
        appender.append(logEvent);
        Message<byte[]> item;
        synchronized (history) {
            assertEquals(1, history.size());
            item = history.get(0);
        }
        assertNotNull(item);
        String msgKey = item.getKey();
        assertEquals(msgKey, "key");
        assertEquals(LOG_MESSAGE, new String(item.getData(), StandardCharsets.UTF_8));
    }

    @Test
    public void testAppendWithKeyLookup() {
        final Appender appender = ctx.getConfiguration().getAppender("PulsarAppenderWithKeyLookup");
        final LogEvent logEvent = createLogEvent();
        Date date = new Date();
        SimpleDateFormat format = new SimpleDateFormat("dd-MM-yyyy");
        appender.append(logEvent);
        Message<byte[]> item;
        synchronized (history) {
            assertEquals(1, history.size());
            item = history.get(0);
        }
        assertNotNull(item);
        String keyValue = format.format(date);
        assertEquals(item.getKey(), keyValue);
        assertEquals(LOG_MESSAGE, new String(item.getData(), StandardCharsets.UTF_8));
    }

    private LogEvent deserializeLogEvent(final byte[] data) throws IOException, ClassNotFoundException {
        final ByteArrayInputStream bis = new ByteArrayInputStream(data);
        try (ObjectInput ois = new ObjectInputStream(bis)) {
            return (LogEvent) ois.readObject();
        }
    }

}