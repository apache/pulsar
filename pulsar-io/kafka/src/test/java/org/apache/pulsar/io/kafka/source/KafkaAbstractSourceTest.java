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

package org.apache.pulsar.io.kafka.source;


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.io.core.SourceContext;
import org.apache.pulsar.io.kafka.KafkaAbstractSource;
import org.apache.pulsar.io.kafka.KafkaSourceConfig;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.expectThrows;
import static org.testng.Assert.fail;


public class KafkaAbstractSourceTest {

    private static class DummySource extends KafkaAbstractSource<String> {

        @Override
        public KafkaRecord buildRecord(ConsumerRecord<Object, Object> consumerRecord) {
            KafkaRecord record = new KafkaRecord(consumerRecord,
                    new String((byte[]) consumerRecord.value(), StandardCharsets.UTF_8),
                    Schema.STRING);
            return record;
        }

    }

    @Test
    public void testInvalidConfigWillThrownException() throws Exception {
        KafkaAbstractSource source = new DummySource();
        SourceContext ctx = mock(SourceContext.class);
        Map<String, Object> config = new HashMap<>();
        Assert.ThrowingRunnable openAndClose = ()->{
            try {
                source.open(config, ctx);
                fail();
            } finally {
                source.close();
            }
        };
        expectThrows(NullPointerException.class, openAndClose);
        config.put("topic", "topic_1");
        expectThrows(NullPointerException.class, openAndClose);
        config.put("bootstrapServers", "localhost:8080");
        expectThrows(NullPointerException.class, openAndClose);
        config.put("groupId", "test-group");
        config.put("fetchMinBytes", -1);
        expectThrows(IllegalArgumentException.class, openAndClose);
        config.put("fetchMinBytes", 1000);
        config.put("autoCommitEnabled", true);
        config.put("autoCommitIntervalMs", -1);
        expectThrows(IllegalArgumentException.class, openAndClose);
        config.put("autoCommitIntervalMs", 100);
        config.put("sessionTimeoutMs", -1);
        expectThrows(IllegalArgumentException.class, openAndClose);
        config.put("sessionTimeoutMs", 10000);
        config.put("heartbeatIntervalMs", -100);
        expectThrows(IllegalArgumentException.class, openAndClose);
        config.put("heartbeatIntervalMs", 20000);
        expectThrows(IllegalArgumentException.class, openAndClose);
        config.put("heartbeatIntervalMs", 5000);
        config.put("autoOffsetReset", "some-value");
        expectThrows(IllegalArgumentException.class, openAndClose);
        config.put("autoOffsetReset", "earliest");
        source.open(config, ctx);
        source.close();
    }

    @Test
    public final void loadFromYamlFileTest() throws IOException {
        File yamlFile = getFile("kafkaSourceConfig.yaml");
        KafkaSourceConfig config = KafkaSourceConfig.load(yamlFile.getAbsolutePath());
        assertNotNull(config);
        assertEquals("localhost:6667", config.getBootstrapServers());
        assertEquals("test", config.getTopic());
        assertEquals(Long.parseLong("10000"), config.getSessionTimeoutMs());
        assertFalse(config.isAutoCommitEnabled());
        assertEquals("latest", config.getAutoOffsetReset());
        assertNotNull(config.getConsumerConfigProperties());
        Properties props = new Properties();
        props.putAll(config.getConsumerConfigProperties());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, config.getGroupId());
        assertEquals("test-pulsar-consumer", props.getProperty("client.id"));
        assertEquals("SASL_PLAINTEXT", props.getProperty("security.protocol"));
        assertEquals("test-pulsar-io", props.getProperty(ConsumerConfig.GROUP_ID_CONFIG));
    }

    @Test
    public final void loadFromSaslYamlFileTest() throws IOException {
        File yamlFile = getFile("kafkaSourceConfigSasl.yaml");
        KafkaSourceConfig config = KafkaSourceConfig.load(yamlFile.getAbsolutePath());
        assertNotNull(config);
        assertEquals(config.getBootstrapServers(), "localhost:6667");
        assertEquals(config.getTopic(), "test");
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
