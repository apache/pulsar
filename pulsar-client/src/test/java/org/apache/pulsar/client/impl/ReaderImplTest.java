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
package org.apache.pulsar.client.impl;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.conf.ReaderConfigurationData;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.mockito.Mockito.when;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

public class ReaderImplTest {
    ReaderImpl<byte[]> reader;
    private ExecutorService executorService;

    @BeforeMethod
    void setupReader() {
        PulsarClientImpl mockedClient = ClientTestFixtures.createPulsarClientMockWithMockedClientCnx();
        ReaderConfigurationData<byte[]> readerConfiguration = new ReaderConfigurationData<>();
        readerConfiguration.setTopicName("topicName");
        executorService = Executors.newSingleThreadExecutor();
        when(mockedClient.getInternalExecutorService()).thenReturn(executorService);
        CompletableFuture<Consumer<byte[]>> consumerFuture = new CompletableFuture<>();
        reader = new ReaderImpl<>(mockedClient, readerConfiguration, ClientTestFixtures.createMockedExecutorProvider(),
                consumerFuture, Schema.BYTES);
    }

    @AfterMethod
    public void clean() {
        if (executorService != null) {
            executorService.shutdownNow();
            executorService = null;
        }
    }

    @Test
    void shouldSupportCancellingReadNextAsync() {
        // given
        CompletableFuture<Message<byte[]>> future = reader.readNextAsync();
        Awaitility.await().untilAsserted(() -> {
            assertNotNull(reader.getConsumer().peekPendingReceive());
        });

        // when
        future.cancel(false);

        // then
        assertNull(reader.getConsumer().peekPendingReceive());
    }
}
