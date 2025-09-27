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
import io.netty.resolver.dns.SequentialDnsServerAddressStreamProvider;
import io.netty.util.HashedWheelTimer;
import java.util.Optional;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.util.ExecutorProvider;
import org.apache.pulsar.client.util.ScheduledExecutorProvider;
import org.apache.pulsar.common.util.netty.EventLoopUtil;

class PulsarClientResourcesConfigurer {
    static final String NAME_TIMER = "pulsar-timer";
    static final String POOL_NAME_LOOKUP_EXECUTOR = "pulsar-client-lookup";
    static final String POOL_NAME_INTERNAL_EXECUTOR = "pulsar-client-internal";
    static final String POOL_NAME_LISTENER_EXECUTOR = "pulsar-external-listener";
    static final String POOL_NAME_SCHEDULED_EXECUTOR = "pulsar-client-scheduled";
    static final String POOL_NAME_EVENT_LOOP_GROUP = "pulsar-client-io";
    static final int LOOKUP_EXECUTOR_NUM_THREADS = 1;

    static HashedWheelTimer createTimer() {
        return createTimerInternal(NAME_TIMER, 1L, TimeUnit.MILLISECONDS);
    }

    private static HashedWheelTimer createTimerInternal(String poolName, long tickDuration, TimeUnit timeUnit) {
        return new HashedWheelTimer(createThreadFactory(poolName), tickDuration, timeUnit);
    }

    static HashedWheelTimer createTimer(PulsarClientSharedResourcesBuilderImpl.TimerResourceConfig resourceConfig) {
        if (resourceConfig == null) {
            resourceConfig = new PulsarClientSharedResourcesBuilderImpl.TimerResourceConfig();
        }
        return createTimerInternal(resourceConfig.name, resourceConfig.tickDuration,
                resourceConfig.tickDurationTimeUnit);
    }

    static ExecutorProvider createLookupExecutorProvider() {
        return createExecutorProviderInternal(LOOKUP_EXECUTOR_NUM_THREADS, POOL_NAME_LOOKUP_EXECUTOR);
    }

    static ExecutorProvider createLookupExecutorProviderWithResourceConfig(
            PulsarClientSharedResourcesBuilderImpl.ThreadPoolResourceConfig resourceConfig) {
        return createExecutorProviderInternal(resourceConfig, POOL_NAME_LOOKUP_EXECUTOR);
    }

    private static ExecutorProvider createExecutorProviderInternal(int numThreads, String poolName) {
        return createExecutorProviderInternal(numThreads, poolName, Thread.currentThread().isDaemon());
    }

    private static ExecutorProvider createExecutorProviderInternal(int numThreads, String poolName, boolean daemon) {
        return new ExecutorProvider(numThreads, poolName, daemon);
    }

    static ExecutorProvider createInternalExecutorProvider(ClientConfigurationData conf) {
        return createExecutorProviderInternal(conf.getNumIoThreads(), POOL_NAME_INTERNAL_EXECUTOR);
    }

    static ExecutorProvider createInternalExecutorProviderWithResourceConfig(
            PulsarClientSharedResourcesBuilderImpl.ThreadPoolResourceConfig resourceConfig) {
        return createExecutorProviderInternal(resourceConfig, POOL_NAME_INTERNAL_EXECUTOR);
    }

    static ExecutorProvider createExternalExecutorProvider(ClientConfigurationData conf) {
        return createExecutorProviderInternal(conf.getNumListenerThreads(), POOL_NAME_LISTENER_EXECUTOR);
    }

    static ExecutorProvider createExternalExecutorProviderWithResourceConfig(
            PulsarClientSharedResourcesBuilderImpl.ThreadPoolResourceConfig resourceConfig) {
        return createExecutorProviderInternal(resourceConfig, POOL_NAME_LISTENER_EXECUTOR);
    }

    private static ExecutorProvider createExecutorProviderInternal(
            PulsarClientSharedResourcesBuilderImpl.ThreadPoolResourceConfig resourceConfig, String defaultPoolName) {
        if (resourceConfig == null) {
            resourceConfig = new PulsarClientSharedResourcesBuilderImpl.ThreadPoolResourceConfig();
        }
        String poolName = StringUtils.isNotBlank(resourceConfig.name) ? resourceConfig.name : defaultPoolName;
        return createExecutorProviderInternal(resourceConfig.numberOfThreads, poolName, resourceConfig.daemon);
    }

    static ScheduledExecutorProvider createScheduledExecutorProvider(ClientConfigurationData conf) {
        return new ScheduledExecutorProvider(conf.getNumIoThreads(), POOL_NAME_SCHEDULED_EXECUTOR);
    }

    static ScheduledExecutorProvider createScheduledExecutorProviderWithResourceConfig(
            PulsarClientSharedResourcesBuilderImpl.ThreadPoolResourceConfig resourceConfig) {
        if (resourceConfig == null) {
            resourceConfig = new PulsarClientSharedResourcesBuilderImpl.ThreadPoolResourceConfig();
        }
        String poolName =
                StringUtils.isNotBlank(resourceConfig.name) ? resourceConfig.name : POOL_NAME_SCHEDULED_EXECUTOR;
        return new ScheduledExecutorProvider(resourceConfig.numberOfThreads, poolName,  resourceConfig.daemon);
    }

    static EventLoopGroup createEventLoopGroup(ClientConfigurationData conf) {
        ThreadFactory threadFactory = createThreadFactory(POOL_NAME_EVENT_LOOP_GROUP);
        return EventLoopUtil.newEventLoopGroup(conf.getNumIoThreads(), conf.isEnableBusyWait(), threadFactory);
    }

    static EventLoopGroup createEventLoopGroupWithResourceConfig(
            PulsarClientSharedResourcesBuilderImpl.EventLoopResourceConfig
                    eventLoopResourceConfig) {
        if (eventLoopResourceConfig == null) {
            eventLoopResourceConfig = new PulsarClientSharedResourcesBuilderImpl.EventLoopResourceConfig();
        }
        ThreadFactory threadFactory = createThreadFactory(eventLoopResourceConfig.name, eventLoopResourceConfig.daemon);
        return EventLoopUtil.newEventLoopGroup(eventLoopResourceConfig.numberOfThreads,
                eventLoopResourceConfig.enableBusyWait, threadFactory);
    }

    static ThreadFactory createThreadFactory(String poolName) {
        return createThreadFactory(poolName, Thread.currentThread().isDaemon());
    }

    static ThreadFactory createThreadFactory(String poolName, boolean daemon) {
        return new ExecutorProvider.ExtendedThreadFactory(poolName, daemon);
    }

    static DnsResolverGroupImpl createDnsResolverGroup(ClientConfigurationData conf,
                                                                         EventLoopGroup eventLoopGroup) {
        return new DnsResolverGroupImpl(eventLoopGroup, conf);
    }

    static DnsResolverGroupImpl createDnsResolverGroupWithResourceConfig(
            PulsarClientSharedResourcesBuilderImpl.DnsResolverResourceConfig resourceConfig,
            EventLoopGroup eventLoopGroup) {
        if (resourceConfig == null) {
            resourceConfig = new PulsarClientSharedResourcesBuilderImpl.DnsResolverResourceConfig();
        }
        return new DnsResolverGroupImpl(eventLoopGroup, Optional.ofNullable(resourceConfig.localAddress),
                Optional.ofNullable(resourceConfig.serverAddresses).map(SequentialDnsServerAddressStreamProvider::new));
    }
}
