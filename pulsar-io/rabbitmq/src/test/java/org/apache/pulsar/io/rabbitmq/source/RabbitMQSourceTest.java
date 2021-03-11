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
package org.apache.pulsar.io.rabbitmq.source;

import org.apache.pulsar.io.rabbitmq.RabbitMQBrokerManager;
import org.apache.pulsar.io.rabbitmq.RabbitMQSource;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

public class RabbitMQSourceTest {

    private RabbitMQBrokerManager rabbitMQBrokerManager;

    @BeforeMethod
    public void setUp() throws Exception {
        rabbitMQBrokerManager = new RabbitMQBrokerManager();
        rabbitMQBrokerManager.startBroker("5672");
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        rabbitMQBrokerManager.stopBroker();
    }

    @Test
    public void TestOpenAndWriteSink() {
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
        configs.put("prefetchCount", "0");
        configs.put("prefetchGlobal", "false");
        configs.put("passive", "false");

        RabbitMQSource source = new RabbitMQSource();

        // open should success
        // rabbitmq service may need time to initialize
        Awaitility.await().ignoreExceptions().untilAsserted(() -> source.open(configs, null));
    }

}
