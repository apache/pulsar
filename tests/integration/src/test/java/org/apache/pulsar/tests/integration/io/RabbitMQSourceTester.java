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
package org.apache.pulsar.tests.integration.io;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.apache.pulsar.tests.integration.containers.RabbitMQContainer;
import org.apache.pulsar.tests.integration.io.sources.SourceTester;

import static org.apache.pulsar.tests.integration.io.RabbitMQSinkTester.createConnectionFactory;

import java.util.LinkedHashMap;
import java.util.Map;

public class RabbitMQSourceTester extends SourceTester<RabbitMQContainer> {
    private RabbitMQContainer serviceContainer;
    private final String exchangeName = "test-src-exchange";
    private final String queueName = "test-src-queue";

    public RabbitMQSourceTester(String networkAlias) {
        super("rabbitmq");

        sourceConfig.put("connectionName", "test-source-connection");
        sourceConfig.put("host", networkAlias);
        sourceConfig.put("port", RabbitMQContainer.PORTS[0]);
        sourceConfig.put("queueName", queueName);
    }

    @Override
    public void setServiceContainer(RabbitMQContainer serviceContainer) {
        this.serviceContainer = serviceContainer;
    }

    @Override
    public void prepareSource() throws Exception {
    }

    @Override
    public void prepareInsertEvent() throws Exception {
        // pass
    }

    @Override
    public void prepareDeleteEvent() throws Exception {
        // pass
    }

    @Override
    public void prepareUpdateEvent() throws Exception {
        // pass
    }

    @Override
    public Map<String, String> produceSourceMessages(int numMessages) throws Exception {
        ConnectionFactory connectionFactory = createConnectionFactory(serviceContainer);
        try (Connection connection = connectionFactory.newConnection("rabbitmq-source-tester");
             Channel channel = connection.createChannel()) {
            // the queue declaration has to be aligned with that in RabbitMQSource
            channel.queueDeclare(queueName, false, false, false, null);
            // use topic mode exchange in order to publish messages with any keys
            channel.exchangeDeclare(exchangeName, BuiltinExchangeType.TOPIC);
            channel.queueBind(queueName, exchangeName, "#");

            Map<String, String> values = new LinkedHashMap<>();
            for (int i = 0; i < numMessages; i++) {
                String key = "rb-key-" + i;
                String value = "rb-value-" + i;
                values.put(key, value);
                channel.basicPublish(exchangeName, key, null, value.getBytes());
            }
            return values;
        }
    }
}
