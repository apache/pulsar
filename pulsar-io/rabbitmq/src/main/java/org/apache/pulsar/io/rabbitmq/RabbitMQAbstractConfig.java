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

import com.google.common.base.Preconditions;
import com.rabbitmq.client.ConnectionFactory;
import java.io.Serializable;
import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.pulsar.io.core.annotations.FieldDoc;

/**
 * Configuration object for all RabbitMQ components.
 */
@Data
@Accessors(chain = true)
public class RabbitMQAbstractConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    @FieldDoc(
        required = true,
        defaultValue = "",
        help = "The connection name used for connecting to RabbitMQ")
    private String connectionName;

    @FieldDoc(
        required = true,
        defaultValue = "",
        help = "The RabbitMQ host to connect to")
    private String host;

    @FieldDoc(
        required = true,
        defaultValue = "5672",
        help = "The RabbitMQ port to connect to")
    private int port = 5672;

    @FieldDoc(
        required = true,
        defaultValue = "/",
        help = "The virtual host used for connecting to RabbitMQ")
    private String virtualHost = "/";

    @FieldDoc(
        required = false,
        defaultValue = "guest",
        sensitive = true,
        help = "The username used to authenticate to RabbitMQ")
    private String username = "guest";

    @FieldDoc(
        required = false,
        defaultValue = "guest",
        sensitive = true,
        help = "The password used to authenticate to RabbitMQ")
    private String password = "guest";

    @FieldDoc(
            required = false,
            defaultValue = "",
            help = "The RabbitMQ queue name from which messages should be read from or written to")
    private String queueName;

    @FieldDoc(
        required = false,
        defaultValue = "0",
        help = "Initially requested maximum channel number. 0 for unlimited")
    private int requestedChannelMax = 0;

    @FieldDoc(
        required = false,
        defaultValue = "0",
        help = "Initially requested maximum frame size, in octets. 0 for unlimited")
    private int requestedFrameMax = 0;

    @FieldDoc(
        required = false,
        defaultValue = "60000",
        help = "Connection TCP establishment timeout in milliseconds. 0 for infinite")
    private int connectionTimeout = 60000;

    @FieldDoc(
        required = false,
        defaultValue = "10000",
        help = "The AMQP0-9-1 protocol handshake timeout in milliseconds")
    private int handshakeTimeout = 10000;

    @FieldDoc(
        required = false,
        defaultValue = "60",
        help = "The requested heartbeat timeout in seconds")
    private int requestedHeartbeat = 60;

    public void validate() {
        Preconditions.checkNotNull(host, "host property not set.");
        Preconditions.checkNotNull(port, "port property not set.");
        Preconditions.checkNotNull(virtualHost, "virtualHost property not set.");
        Preconditions.checkNotNull(connectionName, "connectionName property not set.");
    }

    public ConnectionFactory createConnectionFactory() {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost(this.host);
        connectionFactory.setUsername(this.username);
        connectionFactory.setPassword(this.password);
        connectionFactory.setVirtualHost(this.virtualHost);
        connectionFactory.setRequestedChannelMax(this.requestedChannelMax);
        connectionFactory.setRequestedFrameMax(this.requestedFrameMax);
        connectionFactory.setConnectionTimeout(this.connectionTimeout);
        connectionFactory.setHandshakeTimeout(this.handshakeTimeout);
        connectionFactory.setRequestedHeartbeat(this.requestedHeartbeat);
        connectionFactory.setPort(this.port);
        return connectionFactory;
    }
}