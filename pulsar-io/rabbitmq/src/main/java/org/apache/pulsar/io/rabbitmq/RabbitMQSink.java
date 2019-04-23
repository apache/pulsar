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
package org.apache.pulsar.io.rabbitmq;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.core.annotations.Connector;
import org.apache.pulsar.io.core.annotations.IOType;

import java.io.IOException;
import java.util.Map;

/**
 * A Simple RabbitMQ sink, which transfer records from Pulsar to RabbitMQ.
 * This class expects records from Pulsar to have values that are stored as bytes or string.
 */
@Connector(
    name = "rabbitmq",
    type = IOType.SINK,
    help = "A sink connector is used for moving messages from Pulsar to RabbitMQ.",
    configClass = RabbitMQSinkConfig.class
)
@Slf4j
public class RabbitMQSink implements Sink<byte[]> {

    private Connection rabbitMQConnection;
    private Channel rabbitMQChannel;
    private RabbitMQSinkConfig rabbitMQSinkConfig;
    private String exchangeName;
    private String routingKey;

    @Override
    public void open(Map<String, Object> config, SinkContext sinkContext) throws Exception {
        rabbitMQSinkConfig = RabbitMQSinkConfig.load(config);
        rabbitMQSinkConfig.validate();

        ConnectionFactory connectionFactory = rabbitMQSinkConfig.createConnectionFactory();
        rabbitMQConnection = connectionFactory.newConnection(rabbitMQSinkConfig.getConnectionName());
        log.info("A new connection to {}:{} has been opened successfully.",
            rabbitMQConnection.getAddress().getCanonicalHostName(),
            rabbitMQConnection.getPort()
        );

        exchangeName = rabbitMQSinkConfig.getExchangeName();
        routingKey = rabbitMQSinkConfig.getRoutingKey();

        rabbitMQChannel = rabbitMQConnection.createChannel();

        // several clients share a queue
        rabbitMQChannel.exchangeDeclare(exchangeName, BuiltinExchangeType.DIRECT, true);
        rabbitMQChannel.queueDeclare(rabbitMQSinkConfig.getQueueName(), true, false, false, null);
        rabbitMQChannel.queueBind(rabbitMQSinkConfig.getQueueName(), exchangeName, routingKey);
    }

    @Override
    public void write(Record<byte[]> record) {
        byte[] value = record.getValue();
        try {
            rabbitMQChannel.basicPublish(exchangeName, routingKey, null, value);
            record.ack();
        } catch (IOException e) {
            record.fail();
            log.warn("Failed to publish the message to RabbitMQ ", e);
        }
    }

    @Override
    public void close() throws Exception {
        if (rabbitMQChannel != null) {
            rabbitMQChannel.close();
        }
        if (rabbitMQConnection != null) {
            rabbitMQConnection.close();
        }
    }
}
