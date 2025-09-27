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

import static org.apache.pulsar.client.api.PulsarClientSharedResources.SharedResource;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Builder for configuring and creating {@link PulsarClientSharedResources}.
 * <p>
 * Shared resources allow multiple PulsarClient instances to reuse common components
 * (for example executors, timers, DNS resolver, event loop, connection pool),
 * reducing memory footprint and thread count when many clients are created in the same JVM.
 * This allows creating a large number of PulsarClient instances in a single JVM without wasting resources.
 * Sharing the DNS resolver and cache will help reduce the number of DNS lookups performed by the clients
 * and improve latency and reduce load on the DNS servers.
 * <p>
 * Typical usage:
 * <pre>{@code
 * PulsarClientSharedResources shared = PulsarClientSharedResources
 *     .builder()
 *     // shared resources can be configured using a ClientBuilder
 *     .configureResources(PulsarClient.builder().ioThreads(4))
 *     .build();
 *
 * PulsarClient client = PulsarClient.builder()
 *     .sharedResources(shared)
 *     .serviceUrl("pulsar://localhost:6650")
 *     .build();
 *
 * client.close();
 * // it's necessary to close the shared resources after usage
 * shared.close();
 * }</pre>
 */
public interface PulsarClientSharedResourcesBuilder {
    /**
     * Optionally limits the shared resource types to be created and exposed by the resulting
     * {@link PulsarClientSharedResources}. If this method isn't called, resources for all resource types
     * will be created and exposed.
     * Calling this method is additive and the builder will accumulate the shared resource types from all method calls.
     * When specifying {@link SharedResource#DnsResolver}, it is mandatory to also set
     * {@link SharedResource#EventLoopGroup} since the shared DNS resolver requires the event loop group.
     *
     * @param sharedResource one or more resource types to include
     * @return this builder
     */
    PulsarClientSharedResourcesBuilder resourceTypes(SharedResource... sharedResource);

    /**
     * @param sharedResource one or more resource types to include
     * @return this builder
     * @see {@link #resourceTypes(SharedResource...)}
     */
    PulsarClientSharedResourcesBuilder resourceTypes(Collection<SharedResource> sharedResource);

    /**
     * Share only the configured resources. It's not allowed to use {@link #resourceTypes(SharedResource...)} when
     * this method is called. When configuring {@link SharedResource#DnsResolver}, it is mandatory to also
     * configure {@link SharedResource#EventLoopGroup} since the shared DNS resolver requires the event loop group.
     * @return this builder instance for method chaining
     */
    PulsarClientSharedResourcesBuilder shareConfigured();

    /**
     * Builds the {@link PulsarClientSharedResources} instance. It is necessary to call
     * {@link PulsarClientSharedResources#close()} on the instance to release resources. All clients must be closed
     * before closing the shared resources.
     *
     * @return the shared resources to be used in {@link ClientBuilder#sharedResources(PulsarClientSharedResources)}
     */
    PulsarClientSharedResources build();

    /**
     * Configures a thread pool for the specified shared resource type.
     *
     * @param sharedResource the type of shared resource to configure
     * @param configurer     a consumer that configures the thread pool settings
     * @return this builder instance for method chaining
     */
    PulsarClientSharedResourcesBuilder configureThreadPool(SharedResource sharedResource,
                                                           Consumer<ThreadPoolConfig> configurer);

    /**
     * Configures the event loop group settings.
     *
     * @param configurer a consumer that configures the event loop group settings
     * @return this builder instance for method chaining
     */
    PulsarClientSharedResourcesBuilder configureEventLoop(Consumer<EventLoopGroupConfig> configurer);

    /**
     * Configures the DNS resolver settings.
     *
     * @param configurer a consumer that configures the DNS resolver settings
     * @return this builder instance for method chaining
     */
    PulsarClientSharedResourcesBuilder configureDnsResolver(Consumer<DnsResolverConfig> configurer);

    /**
     * Configures the timer settings.
     *
     * @param configurer a consumer that configures the timer settings
     * @return this builder instance for method chaining
     */
    PulsarClientSharedResourcesBuilder configureTimer(Consumer<TimerConfig> configurer);

    /**
     * Configuration interface for thread pool settings.
     */
    interface ThreadPoolConfig {
        /**
         * Sets the name of the thread pool.
         *
         * @param name the name to set for the thread pool
         * @return this config instance for method chaining
         */
        ThreadPoolConfig name(String name);

        /**
         * Sets the number of threads in the thread pool.
         *
         * @param numberOfThreads the number of threads to use
         * @return this config instance for method chaining
         */
        ThreadPoolConfig numberOfThreads(int numberOfThreads);

        /**
         * Sets whether the threads should be daemon threads.
         *
         * @param daemon true if the threads should be daemon threads, false otherwise
         * @return this config instance for method chaining
         */
        ThreadPoolConfig daemon(boolean daemon);
    }

    /**
     * Configuration interface for event loop group settings.
     */
    interface EventLoopGroupConfig {
        /**
         * Sets the name of the event loop group.
         *
         * @param name the name to set for the event loop group
         * @return this config instance for method chaining
         */
        EventLoopGroupConfig name(String name);

        /**
         * Sets the number of threads in the event loop.
         *
         * @param numberOfThreads the number of threads to use
         * @return this config instance for method chaining
         */
        EventLoopGroupConfig numberOfThreads(int numberOfThreads);

        /**
         * Sets whether the threads should be daemon threads.
         *
         * @param daemon true if the threads should be daemon threads, false otherwise
         * @return this config instance for method chaining
         */
        EventLoopGroupConfig daemon(boolean daemon);

        /**
         * Enables or disables busy-wait polling mode.
         *
         * @param enableBusyWait true to enable busy-wait polling, false to disable
         * @return this config instance for method chaining
         */
        EventLoopGroupConfig enableBusyWait(boolean enableBusyWait);
    }

    /**
     * Configuration interface for timer settings.
     */
    interface TimerConfig {
        /**
         * Sets the name of the timer.
         *
         * @param name the name to set for the timer
         * @return this config instance for method chaining
         */
        TimerConfig name(String name);

        /**
         * Sets the tick duration for the timer.
         *
         * @param tickDuration the duration of each tick
         * @param timeUnit     the time unit for the tick duration
         * @return this config instance for method chaining
         */
        TimerConfig tickDuration(long tickDuration, TimeUnit timeUnit);
    }

    /**
     * Configuration interface for DNS resolver settings.
     */
    interface DnsResolverConfig {
        /**
         * Sets the local bind address and port for DNS lookup client.
         *
         * @param localAddress the socket address (address and port) to bind the DNS client to
         * @return the DNS resolver configuration instance for chained calls
         */
        DnsResolverConfig localAddress(InetSocketAddress localAddress);

        /**
         * Sets the DNS server addresses to be used for DNS lookups.
         *
         * @param addresses collection of DNS server socket addresses (address and port)
         * @return the DNS resolver configuration instance for chained calls
         */
        DnsResolverConfig serverAddresses(Collection<InetSocketAddress> addresses);
    }
}