/*
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
package org.apache.pulsar.client.impl;

import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.net.InetSocketAddress;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.metrics.InstrumentProvider;
import org.apache.pulsar.common.util.netty.EventLoopUtil;

/**
 * Implementation of {@link EndpointChecker} that uses a binary protocol to check the health of endpoints.
 * check service url for {@link BinaryProtoLookupService}.
 */
@Slf4j
class BinaryProtoEndpointCheckerImpl implements EndpointChecker {
    private final ConnectionPool connectionPool;
    private final long healthCheckTimeoutMs;

    BinaryProtoEndpointCheckerImpl(long healthCheckTimeoutMs) {
        if (healthCheckTimeoutMs <= 0) {
            throw new IllegalArgumentException("Health check timeout must be greater than zero");
        }
        ClientConfigurationData conf = new ClientConfigurationData();
        // Set connections per broker to 0 to disable connection pooling
        conf.setConnectionsPerBroker(0);
        EventLoopGroup eventLoop =
                EventLoopUtil.newEventLoopGroup(1, false, new DefaultThreadFactory("endpoint-checker-"));
        try {
            this.connectionPool = new ConnectionPool(InstrumentProvider.NOOP, conf, eventLoop, null);
        } catch (Exception e) {
            log.error("Failed to create connection pool", e);
            throw new RuntimeException("Failed to create connection pool", e);
        }
        this.healthCheckTimeoutMs = healthCheckTimeoutMs;
    }

    @Override
    public boolean isHealthy(InetSocketAddress address) {
        if (address == null) {
            return false;
        }
        try {
            connectionPool.getConnection(address)
                    .whenComplete((cnx, throwable) -> {
                        if (cnx != null) {
                            cnx.close();
                        }
                    })
                    .get(healthCheckTimeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS);
            return true;
        } catch (Exception e) {
            log.info("Health check failed for address {}: {}", address, e.getMessage());
            return false;
        }
    }
}
