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

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

import lombok.Data;

import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.PushSource;
import org.apache.pulsar.io.core.SourceContext;
import org.apache.pulsar.io.core.annotations.Connector;
import org.apache.pulsar.io.core.annotations.IOType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple connector to consume messages from a RabbitMQ queue
 */
@Connector(
    name = "rabbitmq",
    type = IOType.SOURCE,
    help = "A simple connector to move messages from a RabbitMQ queue to a Pulsar topic",
    configClass = RabbitMQConfig.class)
public class RabbitMQSource extends PushSource<byte[]> {

    private static Logger logger = LoggerFactory.getLogger(RabbitMQSource.class);

    private Connection rabbitMQConnection;
    private Channel rabbitMQChannel;
    private RabbitMQConfig rabbitMQConfig;

    @Override
    public void open(Map<String, Object> config, SourceContext sourceContext) throws Exception {
        rabbitMQConfig = RabbitMQConfig.load(config);
        if (rabbitMQConfig.getAmqUri() == null
                || rabbitMQConfig.getQueueName() == null) {
            throw new IllegalArgumentException("Required property not set.");
        }
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setUri(rabbitMQConfig.getAmqUri());
        rabbitMQConnection = connectionFactory.newConnection(rabbitMQConfig.getConnectionName());
        logger.info("A new connection to {}:{} has been opened successfully.",
                rabbitMQConnection.getAddress().getCanonicalHostName(),
                rabbitMQConnection.getPort()
        );
        rabbitMQChannel = rabbitMQConnection.createChannel();
        rabbitMQChannel.queueDeclare(rabbitMQConfig.getQueueName(), false, false, false, null);
        com.rabbitmq.client.Consumer consumer = new RabbitMQConsumer(this, rabbitMQChannel);
        rabbitMQChannel.basicConsume(rabbitMQConfig.getQueueName(), consumer);
        logger.info("A consumer for queue {} has been successfully started.", rabbitMQConfig.getQueueName());
    }

    @Override
    public void close() throws Exception {
        rabbitMQChannel.close();
        rabbitMQConnection.close();
    }

    private class RabbitMQConsumer extends DefaultConsumer {
        private RabbitMQSource source;

        public RabbitMQConsumer(RabbitMQSource source, Channel channel) {
            super(channel);
            this.source = source;
        }

        @Override
        public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
            source.consume(new RabbitMQRecord(Optional.ofNullable(envelope.getRoutingKey()), body));
        }
    }

    @Data
    static private class RabbitMQRecord implements Record<byte[]> {
        private final Optional<String> key;
        private final byte[] value;
    }
}