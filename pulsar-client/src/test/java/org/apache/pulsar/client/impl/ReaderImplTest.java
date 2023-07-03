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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.conf.ReaderConfigurationData;
import org.apache.pulsar.client.impl.crypto.MessageCryptoBc;
import org.apache.pulsar.client.util.ExecutorProvider;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import static org.testng.Assert.assertNotNull;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

public class ReaderImplTest {
    private PulsarClientImpl client;
    private ExecutorProvider executorProvider;
    private ExecutorService internalExecutor;

    @BeforeMethod
    void setupReader() {
        executorProvider = new ExecutorProvider(1, "ReaderImplTest");
        internalExecutor = Executors.newSingleThreadScheduledExecutor();
        client = ClientTestFixtures.createPulsarClientMockWithMockedClientCnx(executorProvider, internalExecutor);
    }

    @AfterMethod
    public void clean() throws Exception {
        if (client != null) {
            client.close();
            client = null;
        }
        if (executorProvider != null) {
            executorProvider.shutdownNow();
            executorProvider = null;
        }
        if (internalExecutor != null) {
            internalExecutor.shutdownNow();
            internalExecutor = null;
        }
    }

    @Test
    void shouldSupportCancellingReadNextAsync() {
        ReaderConfigurationData<byte[]> readerConfiguration = new ReaderConfigurationData<>();
        readerConfiguration.setTopicName("topicName");
        CompletableFuture<Consumer<byte[]>> consumerFuture = new CompletableFuture<>();
        ReaderImpl<byte[]> reader = new ReaderImpl<>(
                client,
                readerConfiguration,
                ClientTestFixtures.createMockedExecutorProvider(),
                consumerFuture,
                Schema.BYTES);

        // given
        CompletableFuture<Message<byte[]>> future = reader.readNextAsync();
        Awaitility.await().untilAsserted(() -> {
            assertTrue(reader.getConsumer().hasNextPendingReceive());
        });

        // when
        future.cancel(false);

        // then
        assertFalse(reader.getConsumer().hasNextPendingReceive());
    }

    @Test
    public void testReaderBuilderWhenMessageCryptoSet() throws PulsarClientException {
        ReaderBuilderImpl<byte[]> builder = new ReaderBuilderImpl<>(client, Schema.BYTES);
        builder.topic("testTopicName");
        builder.startMessageFromRollbackDuration(1, TimeUnit.SECONDS);
        builder.messageCrypto(new MessageCryptoBc("ctx1", true));
        assertNotNull(builder.create());
        assertNotNull(builder.getConf().getMessageCrypto());
    }
}
