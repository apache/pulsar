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
package org.apache.pulsar.io.rabbitmq.sink;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.functions.instance.SinkRecord;
import org.apache.pulsar.io.rabbitmq.RabbitMQBrokerManager;
import org.apache.pulsar.io.rabbitmq.RabbitMQSink;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class RabbitMQSinkTest {
    private RabbitMQBrokerManager rabbitMQBrokerManager;

    @BeforeMethod
    public void setUp() throws Exception {
        rabbitMQBrokerManager = new RabbitMQBrokerManager();
        rabbitMQBrokerManager.startBroker("5673");
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        rabbitMQBrokerManager.stopBroker();
    }

    @Test
    public void TestOpenAndWriteSink() throws Exception {
        Map<String, Object> configs = new HashMap<>();
        configs.put("host", "localhost");
        configs.put("port", "5673");
        configs.put("virtualHost", "default");
        configs.put("username", "guest");
        configs.put("password", "guest");
        configs.put("connectionName", "test-connection");
        configs.put("requestedChannelMax", "0");
        configs.put("requestedFrameMax", "0");
        configs.put("connectionTimeout", "60000");
        configs.put("handshakeTimeout", "10000");
        configs.put("requestedHeartbeat", "60");
        configs.put("exchangeName", "test-exchange");
        configs.put("exchangeType", "fanout");

        RabbitMQSink sink = new RabbitMQSink();

        // open should success
        // rabbitmq service may need time to initialize
        Awaitility.await().ignoreExceptions().untilAsserted(() -> sink.open(configs, null));

        // write should success
        Record<byte[]> record = build("test-topic", "fakeKey", "fakeValue", "fakeRoutingKey");
        sink.write(record);

        sink.close();
    }

    private Record<byte[]> build(String topic, String key, String value, String routingKey) {
        // prepare a SinkRecord
        SinkRecord<byte[]> record = new SinkRecord<>(new Record<byte[]>() {
            @Override
            public Optional<String> getKey() {
                return Optional.empty();
            }

            @Override
            public byte[] getValue() {
                return value.getBytes(StandardCharsets.UTF_8);
            }

            @Override
            public Optional<String> getDestinationTopic() {
                if (topic != null) {
                    return Optional.of(topic);
                } else {
                    return Optional.empty();
                }
            }

            @Override
            public Map<String, String> getProperties() {
                return new HashMap<String, String>() {
                    {
                        put("routingKey", routingKey);
                    }
                };
            }
        }, value.getBytes(StandardCharsets.UTF_8));
        return record;
    }
}
