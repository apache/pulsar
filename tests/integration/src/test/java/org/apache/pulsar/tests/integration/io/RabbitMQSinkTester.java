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

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import lombok.Data;
import org.apache.pulsar.tests.integration.containers.RabbitMQContainer;
import org.apache.pulsar.tests.integration.io.sinks.SinkTester;
import org.apache.pulsar.tests.integration.io.sinks.SinkTester.SinkType;
import org.apache.pulsar.tests.integration.topologies.PulsarCluster;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeoutException;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class RabbitMQSinkTester extends SinkTester<RabbitMQContainer> {
    private final String exchangeName = "test-sink-exchange";
    private final String queueName = "test-sink-queue";
    private final String keyName = "test-key";

    public RabbitMQSinkTester(String networkAlias) {
        super(networkAlias, SinkType.RABBITMQ);

        sinkConfig.put("connectionName", "test-sink-connection");
        sinkConfig.put("host", networkAlias);
        sinkConfig.put("port", RabbitMQContainer.PORTS[0]);
        sinkConfig.put("queueName", queueName);
        sinkConfig.put("exchangeName", exchangeName);
        sinkConfig.put("routingKey", keyName);
    }

    @Override
    protected RabbitMQContainer createSinkService(PulsarCluster cluster) {
        return new RabbitMQContainer(cluster.getClusterName(), networkAlias);
    }

    @Override
    public void prepareSink() throws Exception {
    }

    static ConnectionFactory createConnectionFactory(RabbitMQContainer container) {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost(container.getContainerIpAddress());
        connectionFactory.setPort(container.getMappedPort(RabbitMQContainer.PORTS[0]));

        return connectionFactory;
    }

    @Override
    public void validateSinkResult(Map<String, String> kvs) {
        ConnectionFactory connectionFactory = createConnectionFactory(serviceContainer);
        try (Connection connection = connectionFactory.newConnection("rabbitmq-sink-tester");
             Channel channel = connection.createChannel()) {
            BlockingQueue<Record> records = new LinkedBlockingQueue<>();
            channel.queueDeclare(queueName, true, false, false, null);
            channel.basicConsume(queueName, new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                    records.add(new Record(envelope.getRoutingKey(), body));
                    channel.basicAck(envelope.getDeliveryTag(), false);
                }
            });
            // keys are discards when  into rabbitMQ
            for (String value : kvs.values()) {
                try {
                    Record record = records.take();
                    assertEquals(record.key, keyName);
                    assertEquals(new String(record.body), value);
                } catch (InterruptedException e) {
                    break;
                }
            }
        } catch (TimeoutException | IOException e) {
            fail("RabbitMQ Sink test failed", e);
        }
    }

    @Data
    private static class Record {
        private final String key;
        private final byte[] body;
    }
}
