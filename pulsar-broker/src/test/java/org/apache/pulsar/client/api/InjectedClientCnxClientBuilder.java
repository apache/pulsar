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
package org.apache.pulsar.client.api;

import io.netty.channel.EventLoopGroup;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.impl.ClientBuilderImpl;
import org.apache.pulsar.client.impl.ClientCnx;
import org.apache.pulsar.client.impl.ConnectionPool;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.metrics.InstrumentProvider;
import org.apache.pulsar.client.util.ExecutorProvider;
import org.apache.pulsar.common.util.netty.EventLoopUtil;

public class InjectedClientCnxClientBuilder {

    public static PulsarClientImpl create(final ClientBuilderImpl clientBuilder,
                                          final ClientCnxFactory clientCnxFactory) throws Exception {
        ClientConfigurationData conf = clientBuilder.getClientConfigurationData();
        ThreadFactory threadFactory = new ExecutorProvider
                .ExtendedThreadFactory("pulsar-client-io", Thread.currentThread().isDaemon());
        EventLoopGroup eventLoopGroup =
                EventLoopUtil.newEventLoopGroup(conf.getNumIoThreads(), conf.isEnableBusyWait(), threadFactory);

        // Inject into ClientCnx.
        ConnectionPool pool = new ConnectionPool(InstrumentProvider.NOOP, conf, eventLoopGroup,
                () -> clientCnxFactory.generate(conf, eventLoopGroup), null);

        return new InjectedClientCnxPulsarClientImpl(conf, eventLoopGroup, pool);
    }

    public interface ClientCnxFactory {

        ClientCnx generate(ClientConfigurationData conf, EventLoopGroup eventLoopGroup);
    }

    @Slf4j
    private static class InjectedClientCnxPulsarClientImpl extends PulsarClientImpl {

        public InjectedClientCnxPulsarClientImpl(ClientConfigurationData conf, EventLoopGroup eventLoopGroup,
                                                 ConnectionPool pool)
                throws PulsarClientException {
            super(conf, eventLoopGroup, pool);
        }

        @Override
        public CompletableFuture<Void> closeAsync() {
            return super.closeAsync().handle((v, ex) -> {
                try {
                    getCnxPool().close();
                } catch (Exception e) {
                    log.warn("Failed to close cnx pool", e);
                }
                try {
                    eventLoopGroup.shutdownGracefully().get(10, TimeUnit.SECONDS);
                } catch (Exception e) {
                    log.warn("Failed to shutdown event loop group", e);
                }
                return null;
            });
        }
    }
}
