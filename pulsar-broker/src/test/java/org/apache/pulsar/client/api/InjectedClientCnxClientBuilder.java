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
package org.apache.pulsar.client.api;

import io.netty.channel.EventLoopGroup;
import java.util.concurrent.ThreadFactory;
import org.apache.pulsar.client.impl.ClientBuilderImpl;
import org.apache.pulsar.client.impl.ClientCnx;
import org.apache.pulsar.client.impl.ConnectionPool;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
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
        ConnectionPool pool = new ConnectionPool(conf, eventLoopGroup,
                () -> clientCnxFactory.generate(conf, eventLoopGroup));

        return new PulsarClientImpl(conf, eventLoopGroup, pool);
    }

    public interface ClientCnxFactory {

        ClientCnx generate(ClientConfigurationData conf, EventLoopGroup eventLoopGroup);
    }
}
