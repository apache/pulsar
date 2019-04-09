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

import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.functions.instance.SinkRecord;
import org.apache.pulsar.io.rabbitmq.RabbitMQBrokerManager;
import org.apache.pulsar.io.rabbitmq.RabbitMQSink;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class RabbitMQSinkTest {
    private RabbitMQBrokerManager rabbitMQBrokerManager;

    @BeforeMethod
    public void setUp() throws Exception {
        rabbitMQBrokerManager = new RabbitMQBrokerManager();
        rabbitMQBrokerManager.startBroker();
    }

    @AfterMethod
    public void tearDown() throws Exception {
        rabbitMQBrokerManager.stopBroker();
    }

    @Test
    public void TestOpenAndWriteSink() throws Exception {
        Map<String, Object> configs = new HashMap<>();
        configs.put("host", "localhost");
        configs.put("port", "5672");
        configs.put("virtualHost", "default");
        configs.put("username", "guest");
        configs.put("password", "guest");
        configs.put("queueName", "test-queue");
        configs.put("connectionName", "test-connection");
        configs.put("requestedChannelMax", "0");
        configs.put("requestedFrameMax", "0");
        configs.put("connectionTimeout", "60000");
        configs.put("handshakeTimeout", "10000");
        configs.put("requestedHeartbeat", "60");
        configs.put("exchangeName", "test-exchange");
        configs.put("routingKey", "test-key");

        RabbitMQSink sink = new RabbitMQSink();

        // open should success
        sink.open(configs, null);

        // write should success
        Record<String> record = build("test-topic", "fakeKey", "fakeValue");
        sink.write(record);

        sink.close();
    }

    private Record<String> build(String topic, String key, String value) {
        // prepare a SinkRecord
        SinkRecord<String> record = new SinkRecord<>(new Record<String>() {
            @Override
            public Optional<String> getKey() {
                return Optional.empty();
            }

            @Override
            public String getValue() {
                return key;
            }

            @Override
            public Optional<String> getDestinationTopic() {
                if (topic != null) {
                    return Optional.of(topic);
                } else {
                    return Optional.empty();
                }
            }
        }, value);
        return record;
    }
}
