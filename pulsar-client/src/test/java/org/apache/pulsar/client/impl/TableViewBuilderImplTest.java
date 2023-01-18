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

import org.apache.pulsar.client.api.ConsumerCryptoFailureAction;
import org.apache.pulsar.client.api.CryptoKeyReader;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TableView;
import org.apache.pulsar.client.impl.conf.ReaderConfigurationData;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertNotNull;

/**
 * Unit tests of {@link TablewViewBuilderImpl}.
 */
public class TableViewBuilderImplTest {

    private static final String TOPIC_NAME = "testTopicName";
    private PulsarClientImpl client;
    private TableViewBuilderImpl tableViewBuilderImpl;

    @BeforeClass(alwaysRun = true)
    public void setup() {
        Reader reader = mock(Reader.class);
        when(reader.readNextAsync()).thenReturn(CompletableFuture.allOf());
        client = mock(PulsarClientImpl.class);
        when(client.newReader(any(Schema.class)))
            .thenReturn(new ReaderBuilderImpl(client, Schema.BYTES));
        when(client.createReaderAsync(any(ReaderConfigurationData.class), any(Schema.class)))
            .thenReturn(CompletableFuture.completedFuture(reader));
        tableViewBuilderImpl = new TableViewBuilderImpl(client, Schema.BYTES);
    }

    @Test
    public void testTableViewBuilderImpl() throws PulsarClientException {
        TableView tableView = tableViewBuilderImpl.topic(TOPIC_NAME)
            .autoUpdatePartitionsInterval(5, TimeUnit.SECONDS)
            .subscriptionName("testSubscriptionName")
            .cryptoKeyReader(mock(CryptoKeyReader.class))
            .cryptoFailureAction(ConsumerCryptoFailureAction.DISCARD)
            .create();

        assertNotNull(tableView);
    }

    @Test
    public void testTableViewBuilderImplWhenOnlyTopicNameIsSet() throws PulsarClientException {
        TableView tableView = tableViewBuilderImpl.topic(TOPIC_NAME)
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
    public void testTableViewBuilderImplWhenAutoUpdatePartitionsIntervalIsSmallerThanOneSecond() throws PulsarClientException {
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
        TableView tableView = tableViewBuilderImpl.topic(TOPIC_NAME)
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
    public void testTableViewImplWhenDefaultCryptoKeyReaderIsNullMap() throws PulsarClientException {
        tableViewBuilderImpl.topic(TOPIC_NAME).defaultCryptoKeyReader((Map<String, String>) null).create();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testTableViewImplWhenDefaultCryptoKeyReaderIsEmptyMap() throws PulsarClientException {
        tableViewBuilderImpl.topic(TOPIC_NAME).defaultCryptoKeyReader(new HashMap<String, String>()).create();
    }
}
