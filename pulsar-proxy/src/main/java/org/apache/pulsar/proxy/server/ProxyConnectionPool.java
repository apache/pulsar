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
package org.apache.pulsar.proxy.server;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.ClientCnx;
import org.apache.pulsar.client.impl.ConnectionPool;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.EventLoopGroup;

public class ProxyConnectionPool extends ConnectionPool {
    public ProxyConnectionPool(ClientConfigurationData clientConfig, EventLoopGroup eventLoopGroup,
            Supplier<ClientCnx> clientCnxSupplier) throws PulsarClientException {
        super(clientConfig, eventLoopGroup, clientCnxSupplier);
    }

    @Override
    public void close() throws IOException {
        log.info("Closing ProxyConnectionPool.");
        pool.forEach((address, clientCnxPool) -> {
            if (clientCnxPool != null) {
                clientCnxPool.forEach((identifier, clientCnx) -> {
                    if (clientCnx != null && clientCnx.isDone()) {
                        try {
                            clientCnx.get().close();
                        } catch (InterruptedException | ExecutionException e) {
                            log.error("Unable to close get client connection future.", e);
                        }
                    }
                });
            }
        });
        dnsResolver.close();
    }

    private static final Logger log = LoggerFactory.getLogger(ProxyConnectionPool.class);
}
