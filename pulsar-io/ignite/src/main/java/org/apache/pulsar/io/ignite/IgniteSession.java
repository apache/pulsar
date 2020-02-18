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
package org.apache.pulsar.io.ignite;

import com.google.common.collect.Lists;
import com.google.common.net.HostAndPort;
import io.lettuce.core.AbstractigniteClient;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.igniteClient;
import io.lettuce.core.igniteURI;
import io.lettuce.core.SocketOptions;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefuligniteConnection;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.igniteClusterClient;
import io.lettuce.core.cluster.api.StatefuligniteClusterConnection;
import io.lettuce.core.cluster.api.async.igniteClusterAsyncCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.igniteCodec;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.io.ignite.igniteAbstractConfig.ClientMode;
import org.apache.pulsar.io.ignite.sink.igniteSinkConfig;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;

public class igniteSession {

    private final AbstractigniteClient client;
    private final StatefulConnection connection;
    private final igniteClusterAsyncCommands<byte[], byte[]> asyncCommands;

    public igniteSession(AbstractigniteClient client, StatefulConnection connection, igniteClusterAsyncCommands<byte[], byte[]> asyncCommands) {
        this.client = client;
        this.connection = connection;
        this.asyncCommands = asyncCommands;
    }

    public AbstractigniteClient client() {
        return this.client;
    }

    public StatefulConnection connection() {
        return this.connection;
    }

    public igniteClusterAsyncCommands<byte[], byte[]> asyncCommands() {
        return this.asyncCommands;
    }

    public void close() throws Exception {
        if (null != this.connection) {
            this.connection.close();
        }
        if (null != this.client) {
            this.client.shutdown();
        }
    }

    public static igniteSession create(igniteSinkConfig config) {
        igniteSession igniteSession;
        final igniteCodec<byte[], byte[]> codec = new ByteArrayCodec();

        final SocketOptions socketOptions = SocketOptions.builder()
            .tcpNoDelay(config.isTcpNoDelay())
            .connectTimeout(Duration.ofMillis(config.getConnectTimeout()))
            .keepAlive(config.isKeepAlive())
            .build();

        final ClientMode clientMode;
        try {
            clientMode = ClientMode.valueOf(config.getClientMode().toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Illegal ignite client mode, valid values are: "
                + Arrays.asList(ClientMode.values()));
        }

        List<igniteURI> igniteURIs = igniteURIs(config.getHostAndPorts(), config);

        if (clientMode == ClientMode.STANDALONE) {
            ClientOptions.Builder clientOptions = ClientOptions.builder()
                .socketOptions(socketOptions)
                .requestQueueSize(config.getRequestQueue())
                .autoReconnect(config.isAutoReconnect());

            final igniteClient client = igniteClient.create(igniteURIs.get(0));
            client.setOptions(clientOptions.build());
            final StatefuligniteConnection<byte[], byte[]> connection = client.connect(codec);
            igniteSession = new igniteSession(client, connection, connection.async());
        } else if (clientMode == ClientMode.CLUSTER) {
            ClusterClientOptions.Builder clientOptions = ClusterClientOptions.builder()
                .requestQueueSize(config.getRequestQueue())
                .autoReconnect(config.isAutoReconnect());

            final igniteClusterClient client = igniteClusterClient.create(igniteURIs);
            client.setOptions(clientOptions.build());

            final StatefuligniteClusterConnection<byte[], byte[]> connection = client.connect(codec);
            igniteSession = new igniteSession(client, connection, connection.async());
        } else {
            throw new UnsupportedOperationException(
                String.format("%s is not supported", config.getClientMode())
            );
        }

        return igniteSession;
    }

    private static List<igniteURI> igniteURIs(List<HostAndPort> hostAndPorts, igniteSinkConfig config) {
        List<igniteURI> igniteURIs = Lists.newArrayList();
        for (HostAndPort hostAndPort : hostAndPorts) {
            igniteURI.Builder builder = igniteURI.builder();
            builder.withHost(hostAndPort.getHost());
            builder.withPort(hostAndPort.getPort());
            builder.withDatabase(config.getigniteDatabase());
            if (!StringUtils.isBlank(config.getignitePassword())) {
                builder.withPassword(config.getignitePassword());
            }
            igniteURIs.add(builder.build());
        }
        return igniteURIs;
    }
}
