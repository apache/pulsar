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

import static org.apache.pulsar.client.api.PulsarClientSharedResources.ResourceType;

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
     * When specifying {@link ResourceType#dnsResolver}, it is mandatory to also set
     * {@link ResourceType#eventLoopGroup} since the shared DNS resolver requires the event loop group.
     *
     * @param sharedResourceType one or more resource types to include
     * @return this builder
     */
    PulsarClientSharedResourcesBuilder resourceTypes(ResourceType... sharedResourceType);

    /**
     * Applies client-level configuration hints that affect how shared resources are created.
     * <p>
     * For example, IO thread counts or DNS settings on the {@link ClientBuilder} may be used to size or
     * initialize the underlying shared components.
     *
     * @param clientBuilder a client builder carrying relevant configuration
     * @return this builder
     */
    PulsarClientSharedResourcesBuilder configureResources(ClientBuilder clientBuilder);

    /**
     * Builds the {@link PulsarClientSharedResources} instance. It is necessary to call
     * {@link PulsarClientSharedResources#close()} on the instance to release resources. All clients must be closed
     * before closing the shared resources.
     *
     * @return the shared resources to be used in {@link ClientBuilder#sharedResources(PulsarClientSharedResources)}
     */
    PulsarClientSharedResources build();
}