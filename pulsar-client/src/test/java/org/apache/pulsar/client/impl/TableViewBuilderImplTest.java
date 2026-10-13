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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.ConsumerCryptoFailureAction;
import org.apache.pulsar.client.api.CryptoKeyReader;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TableView;
import org.apache.pulsar.client.impl.conf.ReaderConfigurationData;
import org.apache.pulsar.common.topics.TopicCompactionStrategy;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Unit tests of {@link TableViewBuilderImpl}.
 */
public class TableViewBuilderImplTest {

    private static final String TOPIC_NAME = "testTopicName";
    private PulsarClientImpl client;
    private TableViewBuilderImpl<?> tableViewBuilderImpl;
    private CompletableFuture readNextFuture;

    @BeforeClass(alwaysRun = true)
    @SuppressWarnings("unchecked")
    public void setup() {
        Reader<?> reader = mock(Reader.class);
        readNextFuture = new CompletableFuture<>();
        when(reader.readNextAsync()).thenReturn(readNextFuture);
        when(reader.getLastMessageIdsAsync()).thenReturn(CompletableFuture.completedFuture(List.of()));
        client = mock(PulsarClientImpl.class);
        ConnectionPool connectionPool = mock(ConnectionPool.class);
        when(client.getCnxPool()).thenReturn(connectionPool);
        when(client.newReader(any(Schema.class)))
            .thenReturn(new ReaderBuilderImpl(client, Schema.BYTES));
        when(client.createReaderAsync(any(ReaderConfigurationData.class), any(Schema.class)))
            .thenReturn(CompletableFuture.completedFuture(reader));
        tableViewBuilderImpl = new TableViewBuilderImpl(client, Schema.BYTES);
    }

    @AfterClass(alwaysRun = true)
    public void cleanup() {
        if (readNextFuture != null) {
            readNextFuture.completeExceptionally(new PulsarClientException.AlreadyClosedException("Closing test case"));
            readNextFuture = null;
        }
    }

    @Test
    public void testTableViewBuilderImpl() throws PulsarClientException {
        TableView<?> tableView = tableViewBuilderImpl.topic(TOPIC_NAME)
            .autoUpdatePartitionsInterval(5, TimeUnit.SECONDS)
            .subscriptionName("testSubscriptionName")
            .cryptoKeyReader(mock(CryptoKeyReader.class))
            .cryptoFailureAction(ConsumerCryptoFailureAction.DISCARD)
            .create();

        assertNotNull(tableView);
    }

    @Test
    public void testTableViewBuilderImplWhenOnlyTopicNameIsSet() throws PulsarClientException {
        TableView<?> tableView = tableViewBuilderImpl.topic(TOPIC_NAME)
            .create();

        assertNotNull(tableView);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testTableViewBuilderImplWhenTopicIsNullString() throws PulsarClientException {
        tableViewBuilderImpl.topic(null).create();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testTableViewBuilderImplWhenTopicIsEmptyString() throws PulsarClientException {
        tableViewBuilderImpl.topic("").create();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testTableViewBuilderImplWhenAutoUpdatePartitionsIntervalIsSmallerThanOneSecond()
            throws PulsarClientException {
        tableViewBuilderImpl.topic(TOPIC_NAME).autoUpdatePartitionsInterval(100, TimeUnit.MILLISECONDS).create();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testTableViewBuilderImplWhenSubscriptionNameIsNullString() throws PulsarClientException {
        tableViewBuilderImpl.topic(TOPIC_NAME).subscriptionName(null).create();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testTableViewBuilderImplWhenSubscriptionNameIsEmptyString() throws PulsarClientException {
        tableViewBuilderImpl.topic(TOPIC_NAME).subscriptionName("").create();
    }

    @Test
    public void testTableViewBuilderImplWithCryptoKeyReader() throws PulsarClientException {
        TableView<?> tableView = tableViewBuilderImpl.topic(TOPIC_NAME)
            .cryptoKeyReader(mock(CryptoKeyReader.class))
            .create();

        assertNotNull(tableView);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testTableViewImplWhenDefaultCryptoKeyReaderIsNullString() throws PulsarClientException {
        tableViewBuilderImpl.topic(TOPIC_NAME).defaultCryptoKeyReader((String) null).create();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testTableViewImplWhenDefaultCryptoKeyReaderIsEmptyString() throws PulsarClientException {
        tableViewBuilderImpl.topic(TOPIC_NAME).defaultCryptoKeyReader("").create();
    }

    @Test(expectedExceptions = NullPointerException.class)
    @SuppressWarnings("unchecked")
    public void testTableViewImplWhenDefaultCryptoKeyReaderIsNullMap() throws PulsarClientException {
        tableViewBuilderImpl.topic(TOPIC_NAME).defaultCryptoKeyReader((Map<String, String>) null).create();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    @SuppressWarnings("unchecked")
    public void testTableViewImplWhenDefaultCryptoKeyReaderIsEmptyMap() throws PulsarClientException {
        tableViewBuilderImpl.topic(TOPIC_NAME).defaultCryptoKeyReader(new HashMap<String, String>()).create();
    }

    @Test
    public void testCreateMapped() throws PulsarClientException {
        TableView<String> tableView = tableViewBuilderImpl.topic(TOPIC_NAME)
            .createMapped(Message::getKey);

        assertNotNull(tableView);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testCreateMappedWhenMapperIsNull() throws PulsarClientException {
        tableViewBuilderImpl.topic(TOPIC_NAME).createMapped(null);
    }

    @Test
    public void testCreateMappedAsyncWhenMapperIsNull() {
        CompletableFuture<TableView<Object>> future =
            tableViewBuilderImpl.topic(TOPIC_NAME).createMappedAsync(null);

        assertTrue(future.isCompletedExceptionally());
    }

    /**
     * A strategy that loads, so that the builder guard is the only possible source of the rejection.
     */
    public static class NoopStrategy implements TopicCompactionStrategy<byte[]> {
        @Override
        public Schema<byte[]> getSchema() {
            return Schema.BYTES;
        }

        @Override
        public boolean shouldKeepLeft(byte[] prev, byte[] cur) {
            return false;
        }
    }

    @Test
    public void testCreateMappedRejectsCompactionStrategy() {
        TableViewBuilderImpl<byte[]> builder = new TableViewBuilderImpl<>(client, Schema.BYTES);
        builder.topic(TOPIC_NAME)
            .loadConf(Map.of("topicCompactionStrategyClassName", NoopStrategy.class.getName()));

        assertThatThrownBy(() -> builder.createMapped(Message::getKey))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("topicCompactionStrategyClassName");
    }

    @Test
    public void testCreateMappedAsyncRejectsCompactionStrategy() {
        TableViewBuilderImpl<byte[]> builder = new TableViewBuilderImpl<>(client, Schema.BYTES);
        CompletableFuture<TableView<String>> future = builder.topic(TOPIC_NAME)
            .loadConf(Map.of("topicCompactionStrategyClassName", NoopStrategy.class.getName()))
            .createMappedAsync(Message::getKey);

        assertThat(future).isCompletedExceptionally();
        assertThatThrownBy(future::join)
            .cause()
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("topicCompactionStrategyClassName");
    }
}
