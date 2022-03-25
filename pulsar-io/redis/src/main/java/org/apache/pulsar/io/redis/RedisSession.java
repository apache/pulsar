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
package org.apache.pulsar.io.redis;

import com.google.common.collect.Lists;
import com.google.common.net.HostAndPort;
import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.SocketOptions;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.io.redis.RedisAbstractConfig.ClientMode;
import org.apache.pulsar.io.redis.sink.RedisSinkConfig;

public class RedisSession {

    private final AbstractRedisClient client;
    private final StatefulConnection connection;
    private final RedisClusterAsyncCommands<byte[], byte[]> asyncCommands;

    public RedisSession(AbstractRedisClient client, StatefulConnection connection,
                        RedisClusterAsyncCommands<byte[], byte[]> asyncCommands) {
        this.client = client;
        this.connection = connection;
        this.asyncCommands = asyncCommands;
    }

    public AbstractRedisClient client() {
        return this.client;
    }

    public StatefulConnection connection() {
        return this.connection;
    }

    public RedisClusterAsyncCommands<byte[], byte[]> asyncCommands() {
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

    public static RedisSession create(RedisSinkConfig config) {
        RedisSession redisSession;
        final RedisCodec<byte[], byte[]> codec = new ByteArrayCodec();

        final SocketOptions socketOptions = SocketOptions.builder()
            .tcpNoDelay(config.isTcpNoDelay())
            .connectTimeout(Duration.ofMillis(config.getConnectTimeout()))
            .keepAlive(config.isKeepAlive())
            .build();

        final ClientMode clientMode;
        try {
            clientMode = ClientMode.valueOf(config.getClientMode().toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Illegal Redis client mode, valid values are: "
                + Arrays.asList(ClientMode.values()));
        }

        List<RedisURI> redisURIs = redisURIs(config.getHostAndPorts(), config);

        if (clientMode == ClientMode.STANDALONE) {
            ClientOptions.Builder clientOptions = ClientOptions.builder()
                .socketOptions(socketOptions)
                .requestQueueSize(config.getRequestQueue())
                .autoReconnect(config.isAutoReconnect());

            final RedisClient client = RedisClient.create(redisURIs.get(0));
            client.setOptions(clientOptions.build());
            final StatefulRedisConnection<byte[], byte[]> connection = client.connect(codec);
            redisSession = new RedisSession(client, connection, connection.async());
        } else if (clientMode == ClientMode.CLUSTER) {
            ClusterClientOptions.Builder clientOptions = ClusterClientOptions.builder()
                .requestQueueSize(config.getRequestQueue())
                .autoReconnect(config.isAutoReconnect());

            final RedisClusterClient client = RedisClusterClient.create(redisURIs);
            client.setOptions(clientOptions.build());

            final StatefulRedisClusterConnection<byte[], byte[]> connection = client.connect(codec);
            redisSession = new RedisSession(client, connection, connection.async());
        } else {
            throw new UnsupportedOperationException(
                String.format("%s is not supported", config.getClientMode())
            );
        }

        return redisSession;
    }

    private static List<RedisURI> redisURIs(List<HostAndPort> hostAndPorts, RedisSinkConfig config) {
        List<RedisURI> redisURIs = Lists.newArrayList();
        for (HostAndPort hostAndPort : hostAndPorts) {
            RedisURI.Builder builder = RedisURI.builder();
            builder.withHost(hostAndPort.getHost());
            builder.withPort(hostAndPort.getPort());
            builder.withDatabase(config.getRedisDatabase());
            if (!StringUtils.isBlank(config.getRedisPassword())) {
                builder.withPassword(config.getRedisPassword());
            }
            redisURIs.add(builder.build());
        }
        return redisURIs;
    }
}
