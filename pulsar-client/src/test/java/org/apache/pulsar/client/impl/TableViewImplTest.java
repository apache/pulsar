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

import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.pulsar.client.api.CryptoKeyReader;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.ReaderBuilder;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TableView;
import org.apache.pulsar.common.topics.TopicCompactionStrategy;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class TableViewImplTest {

    private PulsarClientImpl client;
    private TableViewConfigurationData data;

    @BeforeClass(alwaysRun = true)
    @SuppressWarnings("unchecked")
    public void setup() {
        client = mock(PulsarClientImpl.class);
        ConnectionPool connectionPool = mock(ConnectionPool.class);
        when(client.getCnxPool()).thenReturn(connectionPool);
        when(client.newReader(any(Schema.class)))
            .thenReturn(new ReaderBuilderImpl(client, Schema.BYTES));

        data = new TableViewConfigurationData();
        data.setTopicName("testTopicName");
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testTableViewImpl() {
        data.setCryptoKeyReader(mock(CryptoKeyReader.class));
        TableView<?> tableView = new TableViewImpl<>(client, Schema.BYTES, data);

        assertNotNull(tableView);
    }
    @DataProvider
    public Object[][] skippedMessage() {
        return new Object[][]{{false}, {true}};
    }

    @Test(timeOut = 10_000, dataProvider = "skippedMessage")
    @SuppressWarnings("unchecked")
    public void testRefreshWaitsForMessageToBeApplied(boolean skipped) throws Exception {
        String topic = "persistent://public/default/refresh-applied";
        PulsarClientImpl client = mock(PulsarClientImpl.class);
        ReaderBuilder<String> builder = mock(ReaderBuilder.class, RETURNS_SELF);
        Reader<String> reader = mock(Reader.class);
        when(client.newReader(Schema.STRING)).thenReturn(builder);
        when(builder.createAsync()).thenReturn(CompletableFuture.completedFuture(reader));
        when(reader.closeAsync()).thenReturn(CompletableFuture.completedFuture(null));
        TopicMessageIdImpl messageId = new TopicMessageIdImpl(topic, new MessageIdImpl(1, 0, -1));
        when(reader.getLastMessageIdsAsync()).thenReturn(
                CompletableFuture.completedFuture(List.of()),
                CompletableFuture.completedFuture(List.of(messageId)));
        CompletableFuture<Message<String>> nextMessage = new CompletableFuture<>();
        when(reader.readNextAsync()).thenReturn(nextMessage, new CompletableFuture<>());
        TableViewConfigurationData conf = new TableViewConfigurationData();
        conf.setTopicName(topic);
        TopicCompactionStrategy<String> strategy = mock(TopicCompactionStrategy.class);
        when(strategy.shouldKeepLeft(any(), any())).thenReturn(skipped);
        TableViewImpl<String> tableView;
        try (var strategies = mockStatic(TopicCompactionStrategy.class)) {
            strategies.when(() -> TopicCompactionStrategy.load(TopicCompactionStrategy.TABLE_VIEW_TAG, null))
                    .thenReturn(strategy);
            tableView = new TableViewImpl<>(client, Schema.STRING, conf);
        }
        tableView.start().get(5, TimeUnit.SECONDS);
        AtomicBoolean callbackRefreshCompleted = new AtomicBoolean();
        tableView.listen((key, value) -> callbackRefreshCompleted.set(tableView.refreshAsync().isDone()));
        doAnswer(invocation -> {
            callbackRefreshCompleted.set(tableView.refreshAsync().isDone());
            return null;
        }).when(strategy).handleSkippedMessage(any(), any());

        CountDownLatch decoding = new CountDownLatch(1);
        CountDownLatch applyMessage = new CountDownLatch(1);
        Message<String> message = mock(Message.class);
        when(message.getTopicName()).thenReturn(topic);
        when(message.getMessageId()).thenReturn(messageId);
        when(message.hasKey()).thenReturn(true);
        when(message.getKey()).thenReturn("key");
        when(message.size()).thenReturn(1);
        when(message.getValue()).thenAnswer(invocation -> {
            decoding.countDown();
            assertTrue(applyMessage.await(5, TimeUnit.SECONDS));
            return "value";
        });
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            var delivery = executor.submit(() -> nextMessage.complete(message));
            assertTrue(decoding.await(5, TimeUnit.SECONDS));
            assertNull(tableView.get("key"));
            var refresh = tableView.refreshAsync();
            assertFalse(refresh.isDone(), "Refresh must not finish before the message updates the table");
            applyMessage.countDown();
            delivery.get(5, TimeUnit.SECONDS);
            refresh.get(5, TimeUnit.SECONDS);
            assertEquals(tableView.get("key"), skipped ? null : "value");
            assertTrue(callbackRefreshCompleted.get(), "A callback must observe the current message as applied");
        } finally {
            applyMessage.countDown();
            executor.shutdownNow();
            tableView.close();
        }
    }

}
