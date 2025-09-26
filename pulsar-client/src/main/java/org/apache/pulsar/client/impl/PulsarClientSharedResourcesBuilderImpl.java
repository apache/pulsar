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

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.pulsar.client.api.PulsarClientSharedResources;
import org.apache.pulsar.client.api.PulsarClientSharedResourcesBuilder;

public class PulsarClientSharedResourcesBuilderImpl implements PulsarClientSharedResourcesBuilder {
    Set<PulsarClientSharedResources.SharedResource> sharedResources = new HashSet<>();
    Map<PulsarClientSharedResources.SharedResource, ResourceConfig> resourceConfigs = new HashMap<>();

    interface ResourceConfig {

    }

    abstract static class NamedResourceConfig<T> implements ResourceConfig {
        String name;

        public T name(String name) {
            this.name = name;
            return (T) this;
        }
    }

    static class ThreadPoolResourceConfig extends NamedResourceConfig<ThreadPoolConfig> implements ThreadPoolConfig {
        int numberOfThreads = Runtime.getRuntime().availableProcessors();
        boolean daemon = Thread.currentThread().isDaemon();

        @Override
        public ThreadPoolConfig numberOfThreads(int numberOfThreads) {
            this.numberOfThreads = numberOfThreads;
            return this;
        }

        @Override
        public ThreadPoolConfig daemon(boolean daemon) {
            this.daemon = daemon;
            return this;
        }
    }

    static class EventLoopResourceConfig extends NamedResourceConfig<EventLoopGroupConfig>
            implements EventLoopGroupConfig {
        int numberOfThreads = Runtime.getRuntime().availableProcessors();
        boolean daemon = Thread.currentThread().isDaemon();
        boolean enableBusyWait;

        EventLoopResourceConfig() {
            name = PulsarClientResourcesConfigurer.POOL_NAME_EVENT_LOOP_GROUP;
        }

        @Override
        public EventLoopGroupConfig numberOfThreads(int numberOfThreads) {
            this.numberOfThreads = numberOfThreads;
            return this;
        }

        @Override
        public EventLoopGroupConfig daemon(boolean daemon) {
            this.daemon = daemon;
            return this;
        }

        @Override
        public EventLoopGroupConfig enableBusyWait(boolean enableBusyWait) {
            this.enableBusyWait = enableBusyWait;
            return this;
        }
    }

    static class TimerResourceConfig extends NamedResourceConfig<TimerConfig> implements TimerConfig {
        long tickDuration = 1L;
        TimeUnit tickDurationTimeUnit  = TimeUnit.MILLISECONDS;

        TimerResourceConfig() {
            name = PulsarClientResourcesConfigurer.NAME_TIMER;
        }

        @Override
        public TimerConfig tickDuration(long tickDuration, TimeUnit timeUnit) {
            this.tickDuration = tickDuration;
            this.tickDurationTimeUnit = timeUnit;
            return this;
        }
    }

    static class DnsResolverResourceConfig implements ResourceConfig, DnsResolverConfig {
        InetSocketAddress bindAddress;
        Collection<InetSocketAddress> serverAddresses;

        @Override
        public DnsResolverConfig bindAddress(InetSocketAddress bindAddress) {
            this.bindAddress = bindAddress;
            return this;
        }

        @Override
        public DnsResolverConfig serverAddresses(Collection<InetSocketAddress> addresses) {
            this.serverAddresses = List.copyOf(addresses);
            return this;
        }
    }

    @Override
    public PulsarClientSharedResourcesBuilder resourceTypes(
            PulsarClientSharedResources.SharedResource... sharedResource) {
        return resourceTypes(List.of(sharedResource));
    }

    @Override
    public PulsarClientSharedResourcesBuilder resourceTypes(
            Collection<PulsarClientSharedResources.SharedResource> sharedResource) {
        sharedResources.addAll(sharedResource);
        return this;
    }

    private <T extends ResourceConfig> T getOrCreateConfig(PulsarClientSharedResources.SharedResource sharedResource) {
        return (T) resourceConfigs.computeIfAbsent(sharedResource, k -> {
            switch (sharedResource.getType()) {
                case EventLoopGroup:
                    return new EventLoopResourceConfig();
                case DnsResolver:
                    return new DnsResolverResourceConfig();
                case ThreadPool:
                    return new ThreadPoolResourceConfig();
                case Timer:
                    return new TimerResourceConfig();
                default:
                    throw new IllegalArgumentException("Unknown resource type: " + sharedResource.getType());
            }
        });
    }

    @Override
    public PulsarClientSharedResourcesBuilder configureThreadPool(
            PulsarClientSharedResources.SharedResource sharedResource, Consumer<ThreadPoolConfig> configurer) {
        if (sharedResource.getType() != PulsarClientSharedResources.SharedResourceType.ThreadPool) {
            throw new IllegalArgumentException("The shared resource " + sharedResource + " doesn't support thread pool"
                    + " configuration");
        }
        configurer.accept(getOrCreateConfig(sharedResource));
        return this;
    }

    @Override
    public PulsarClientSharedResourcesBuilder configureEventLoop(Consumer<EventLoopGroupConfig> configurer) {
        configurer.accept(getOrCreateConfig(PulsarClientSharedResources.SharedResource.EventLoopGroup));
        return this;
    }

    @Override
    public PulsarClientSharedResourcesBuilder configureDnsResolver(Consumer<DnsResolverConfig> configurer) {
        configurer.accept(getOrCreateConfig(PulsarClientSharedResources.SharedResource.DnsResolver));
        return this;
    }

    @Override
    public PulsarClientSharedResourcesBuilder configureTimer(Consumer<TimerConfig> configurer) {
        configurer.accept(getOrCreateConfig(PulsarClientSharedResources.SharedResource.Timer));
        return this;
    }

    @Override
    public PulsarClientSharedResources build() {
        return new PulsarClientSharedResourcesImpl(sharedResources, resourceConfigs);
    }
}
