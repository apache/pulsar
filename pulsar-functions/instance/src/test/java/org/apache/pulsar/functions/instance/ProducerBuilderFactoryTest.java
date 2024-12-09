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
package org.apache.pulsar.functions.instance;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.BatcherBuilder;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.CryptoKeyReader;
import org.apache.pulsar.client.api.EncryptionKeyInfo;
import org.apache.pulsar.client.api.HashingScheme;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.ProducerCryptoFailureAction;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.functions.CryptoConfig;
import org.apache.pulsar.common.functions.ProducerConfig;
import org.mockito.internal.util.MockUtil;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ProducerBuilderFactoryTest {
    private PulsarClient pulsarClient;
    private ProducerBuilder producerBuilder;

    @BeforeMethod
    public void setup() {
        pulsarClient = mock(PulsarClient.class);

        producerBuilder = mock(ProducerBuilder.class);
        doReturn(producerBuilder).when(producerBuilder).blockIfQueueFull(anyBoolean());
        doReturn(producerBuilder).when(producerBuilder).enableBatching(anyBoolean());
        doReturn(producerBuilder).when(producerBuilder).batchingMaxPublishDelay(anyLong(), any());
        doReturn(producerBuilder).when(producerBuilder).compressionType(any());
        doReturn(producerBuilder).when(producerBuilder).hashingScheme(any());
        doReturn(producerBuilder).when(producerBuilder).messageRoutingMode(any());
        doReturn(producerBuilder).when(producerBuilder).messageRouter(any());
        doReturn(producerBuilder).when(producerBuilder).topic(anyString());
        doReturn(producerBuilder).when(producerBuilder).producerName(anyString());
        doReturn(producerBuilder).when(producerBuilder).property(anyString(), anyString());
        doReturn(producerBuilder).when(producerBuilder).properties(any());
        doReturn(producerBuilder).when(producerBuilder).sendTimeout(anyInt(), any());

        doReturn(producerBuilder).when(pulsarClient).newProducer();
        doReturn(producerBuilder).when(pulsarClient).newProducer(any());
    }

    @AfterMethod
    public void tearDown() {
        MockUtil.resetMock(pulsarClient);
        pulsarClient = null;
        MockUtil.resetMock(producerBuilder);
        producerBuilder = null;
        TestCryptoKeyReader.LAST_INSTANCE = null;
    }

    @Test
    public void testCreateProducerBuilder() {
        ProducerBuilderFactory builderFactory = new ProducerBuilderFactory(pulsarClient, null, null, null);
        builderFactory.createProducerBuilder("topic", Schema.STRING, "producerName");
        verifyCommon();
        verifyNoMoreInteractions(producerBuilder);
    }

    private void verifyCommon() {
        verify(pulsarClient).newProducer(Schema.STRING);
        verify(producerBuilder).blockIfQueueFull(true);
        verify(producerBuilder).enableBatching(true);
        verify(producerBuilder).batchingMaxPublishDelay(10, TimeUnit.MILLISECONDS);
        verify(producerBuilder).hashingScheme(HashingScheme.Murmur3_32Hash);
        verify(producerBuilder).messageRoutingMode(MessageRoutingMode.CustomPartition);
        verify(producerBuilder).messageRouter(FunctionResultRouter.of());
        verify(producerBuilder).sendTimeout(0, TimeUnit.SECONDS);
        verify(producerBuilder).topic("topic");
        verify(producerBuilder).producerName("producerName");
    }

    @Test
    public void testCreateProducerBuilderWithDefaultConfigurer() {
        ProducerBuilderFactory builderFactory = new ProducerBuilderFactory(pulsarClient, null, null,
                builder -> builder.property("key", "value"));
        builderFactory.createProducerBuilder("topic", Schema.STRING, "producerName");
        verifyCommon();
        verify(producerBuilder).property("key", "value");
        verifyNoMoreInteractions(producerBuilder);
    }

    @Test
    public void testCreateProducerBuilderWithSimpleProducerConfig() {
        ProducerConfig producerConfig = new ProducerConfig();
        producerConfig.setBatchBuilder("KEY_BASED");
        ProducerBuilderFactory builderFactory = new ProducerBuilderFactory(pulsarClient, producerConfig, null, null);
        builderFactory.createProducerBuilder("topic", Schema.STRING, "producerName");
        verifyCommon();
        verify(producerBuilder).compressionType(CompressionType.LZ4);
        verify(producerBuilder).batcherBuilder(BatcherBuilder.KEY_BASED);
        verifyNoMoreInteractions(producerBuilder);
    }

    @Test
    public void testCreateProducerBuilderWithAdvancedProducerConfig() {
        ProducerConfig producerConfig = new ProducerConfig();
        producerConfig.setBatchBuilder("KEY_BASED");
        producerConfig.setCompressionType(CompressionType.SNAPPY);
        producerConfig.setMaxPendingMessages(5000);
        producerConfig.setMaxPendingMessagesAcrossPartitions(50000);
        CryptoConfig cryptoConfig = new CryptoConfig();
        cryptoConfig.setProducerCryptoFailureAction(ProducerCryptoFailureAction.FAIL);
        cryptoConfig.setEncryptionKeys(new String[]{"key1", "key2"});
        cryptoConfig.setCryptoKeyReaderConfig(Map.of("key", "value"));
        cryptoConfig.setCryptoKeyReaderClassName(TestCryptoKeyReader.class.getName());
        producerConfig.setCryptoConfig(cryptoConfig);
        ProducerBuilderFactory builderFactory = new ProducerBuilderFactory(pulsarClient, producerConfig, null, null);
        builderFactory.createProducerBuilder("topic", Schema.STRING, "producerName");
        verifyCommon();
        verify(producerBuilder).compressionType(CompressionType.SNAPPY);
        verify(producerBuilder).batcherBuilder(BatcherBuilder.KEY_BASED);
        verify(producerBuilder).maxPendingMessages(5000);
        verify(producerBuilder).maxPendingMessagesAcrossPartitions(50000);
        TestCryptoKeyReader lastInstance = TestCryptoKeyReader.LAST_INSTANCE;
        assertNotNull(lastInstance);
        assertEquals(lastInstance.configs, cryptoConfig.getCryptoKeyReaderConfig());
        verify(producerBuilder).cryptoKeyReader(lastInstance);
        verify(producerBuilder).cryptoFailureAction(ProducerCryptoFailureAction.FAIL);
        verify(producerBuilder).addEncryptionKey("key1");
        verify(producerBuilder).addEncryptionKey("key2");
        verifyNoMoreInteractions(producerBuilder);
    }

    public static class TestCryptoKeyReader implements CryptoKeyReader {
        static TestCryptoKeyReader LAST_INSTANCE;
        Map<String, Object> configs;
        public TestCryptoKeyReader(Map<String, Object> configs) {
            this.configs = configs;
            assert LAST_INSTANCE == null;
            LAST_INSTANCE = this;
        }

        @Override
        public EncryptionKeyInfo getPublicKey(String keyName, Map<String, String> metadata) {
            throw new UnsupportedOperationException();
        }

        @Override
        public EncryptionKeyInfo getPrivateKey(String keyName, Map<String, String> metadata) {
            throw new UnsupportedOperationException();
        }
    }
}