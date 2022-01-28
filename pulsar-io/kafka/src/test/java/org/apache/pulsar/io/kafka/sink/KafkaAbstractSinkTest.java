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

package org.apache.pulsar.io.kafka.sink;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.KeyValue;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.kafka.KafkaAbstractSink;
import org.apache.pulsar.io.kafka.KafkaSinkConfig;
import org.slf4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import static org.testng.Assert.*;

public class KafkaAbstractSinkTest {
    private static class DummySink extends KafkaAbstractSink<String, byte[]> {

        @Override
        public KeyValue extractKeyValue(Record record) {
            return new KeyValue<>(record.getKey().orElse(null), record.getValue());
        }
    }

    @FunctionalInterface
    public interface ThrowingRunnable {
        void run() throws Throwable;
    }

    private static <T extends Exception> void expectThrows(Class<T> expectedType, String expectedMessage, ThrowingRunnable runnable) {
        try {
            runnable.run();
            Assert.fail();
        } catch (Throwable e) {
            if (expectedType.isInstance(e)) {
                T ex = expectedType.cast(e);
                assertEquals(expectedMessage, ex.getMessage());
                return;
            }
            throw new AssertionError("Unexpected exception type, expected " + expectedType.getSimpleName() + " but got " + e);
        }
        throw new AssertionError("Expected exception");
    }

    @Test
    public void testInvalidConfigWillThrownException() throws Exception {
        KafkaAbstractSink sink = new DummySink();
        Map<String, Object> config = new HashMap<>();
        SinkContext sc = new SinkContext() {
            @Override
            public int getInstanceId() {
                return 0;
            }

            @Override
            public int getNumInstances() {
                return 0;
            }

            @Override
            public void recordMetric(String metricName, double value) {

            }

            @Override
            public Collection<String> getInputTopics() {
                return null;
            }

            @Override
            public String getTenant() {
                return null;
            }

            @Override
            public String getNamespace() {
                return null;
            }

            @Override
            public String getSinkName() {
                return null;
            }

            @Override
            public Logger getLogger() {
                return null;
            }

            @Override
            public String getSecret(String key) { return null; }

            @Override
            public void incrCounter(String key, long amount) {

            }

            @Override
            public CompletableFuture<Void> incrCounterAsync(String key, long amount) {
                return null;
            }

            @Override
            public long getCounter(String key) {
                return 0;
            }

            @Override
            public CompletableFuture<Long> getCounterAsync(String key) {
                return null;
            }

            @Override
            public void putState(String key, ByteBuffer value) {

            }

            @Override
            public CompletableFuture<Void> putStateAsync(String key, ByteBuffer value) {
                return null;
            }

            @Override
            public ByteBuffer getState(String key) {
                return null;
            }

            @Override
            public CompletableFuture<ByteBuffer> getStateAsync(String key) {
                return null;
            }
            
            @Override
            public void deleteState(String key) {
            	
            }
            
            @Override
            public CompletableFuture<Void> deleteStateAsync(String key) {
            	return null;
            }

            @Override
            public PulsarClient getPulsarClient() {
                return null;
            }
        };
        ThrowingRunnable openAndClose = ()->{
            try {
                sink.open(config, sc);
                fail();
            } finally {
                sink.close();
            }
        };
        expectThrows(NullPointerException.class, "Kafka topic is not set", openAndClose);
        config.put("topic", "topic_2");
        expectThrows(NullPointerException.class, "Kafka bootstrapServers is not set", openAndClose);
        config.put("bootstrapServers", "localhost:6667");
        expectThrows(NullPointerException.class, "Kafka acks mode is not set", openAndClose);
        config.put("acks", "1");
        config.put("batchSize", "-1");
        expectThrows(IllegalArgumentException.class, "Invalid Kafka Producer batchSize : -1", openAndClose);
        config.put("batchSize", "16384");
        config.put("maxRequestSize", "-1");
        expectThrows(IllegalArgumentException.class, "Invalid Kafka Producer maxRequestSize : -1", openAndClose);
        config.put("maxRequestSize", "1048576");
        config.put("acks", "none");
        expectThrows(ConfigException.class, "Invalid value none for configuration acks: String must be one of: all, -1, 0, 1", openAndClose);
        config.put("acks", "1");
        sink.open(config, sc);
        sink.close();
    }

    @Test
    public final void loadFromYamlFileTest() throws IOException {
        File yamlFile = getFile("kafkaSinkConfig.yaml");
        KafkaSinkConfig config = KafkaSinkConfig.load(yamlFile.getAbsolutePath());
        assertNotNull(config);
        assertEquals("localhost:6667", config.getBootstrapServers());
        assertEquals("test", config.getTopic());
        assertEquals("1", config.getAcks());
        assertEquals(Long.parseLong("16384"), config.getBatchSize());
        assertEquals(Long.parseLong("1048576"), config.getMaxRequestSize());
        assertNotNull(config.getProducerConfigProperties());
        Properties props = new Properties();
        props.putAll(config.getProducerConfigProperties());
        props.put(ProducerConfig.ACKS_CONFIG, config.getAcks());
        assertEquals("test-pulsar-producer", props.getProperty("client.id"));
        assertEquals("SASL_PLAINTEXT", props.getProperty("security.protocol"));
        assertEquals("GSSAPI", props.getProperty("sasl.mechanism"));
        assertEquals("1", props.getProperty(ProducerConfig.ACKS_CONFIG));
    }

    @Test
    public final void loadFromSaslYamlFileTest() throws IOException {
        File yamlFile = getFile("kafkaSinkConfigSasl.yaml");
        KafkaSinkConfig config = KafkaSinkConfig.load(yamlFile.getAbsolutePath());
        assertNotNull(config);
        assertEquals(config.getBootstrapServers(), "localhost:6667");
        assertEquals(config.getTopic(), "test");
        assertEquals(config.getAcks(), "1");
        assertEquals(config.getBatchSize(), 16384L);
        assertEquals(config.getMaxRequestSize(), 1048576L);
        assertEquals(config.getSecurityProtocol(), SecurityProtocol.SASL_PLAINTEXT.name);
        assertEquals(config.getSaslMechanism(), "PLAIN");
        assertEquals(config.getSaslJaasConfig(), "org.apache.kafka.common.security.plain.PlainLoginModule required \nusername=\"alice\" \npassword=\"pwd\";");
        assertEquals(config.getSslEndpointIdentificationAlgorithm(), "");
        assertEquals(config.getSslTruststoreLocation(), "/etc/cert.pem");
        assertEquals(config.getSslTruststorePassword(), "cert_pwd");
    }

    private File getFile(String name) {
        ClassLoader classLoader = getClass().getClassLoader();
        return new File(classLoader.getResource(name).getFile());
    }

}
