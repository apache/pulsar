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

import java.util.Set;
import org.apache.pulsar.client.internal.DefaultImplementation;

/**
 * Manages shared resources across multiple PulsarClient instances to optimize resource utilization.
 * This interface provides access to common resources such as thread pools and DNS resolver/cache
 * that can be shared between multiple PulsarClient instances.
 *
 * This allows creating a large number of PulsarClient instances in a single JVM without wasting resources.
 * Sharing the DNS resolver and cache will help reduce the number of DNS lookups performed by the clients
 * and improve latency and reduce load on the DNS servers.
 *
 * <p>Key features:
 * <ul>
 *   <li>Thread pool sharing for various Pulsar operations (IO, timer, lookup, etc.)</li>
 *   <li>Shared DNS resolver and cache</li>
 *   <li>Resource lifecycle management</li>
 * </ul>
 *
 * <p>Usage example:
 * <pre>{@code
 * // To share all possible resources across multiple PulsarClient instances
 * PulsarClientSharedResources sharedResources = PulsarClientSharedResources.builder()
 *     .build();
 * // Use these resources with multiple PulsarClient instances
 * PulsarClient client1 = PulsarClient.builder().sharedResources(sharedResources).build();
 * PulsarClient client2 = PulsarClient.builder().sharedResources(sharedResources).build();
 * client1.close();
 * client2.close();
 * sharedResources.close();
 * }</pre>
 *
 * <p>The instance should be created using the {@link PulsarClientSharedResourcesBuilder},
 * which can be obtained via the {@link #builder()} method.
 *
 * Please notice that the current implementation doesn't cover sharing the HTTP client resources in the
 * Pulsar client implementation. A separate HTTP client instance will be created for each PulsarClient instance
 * when HTTP serviceUrl is provided. It's recommended to use the binary protocol for lookups by providing
 * a pulsar:// or pulsar+ssl:// url to serviceUrl when instantiating the client.
 *
 *
 * @see PulsarClientSharedResourcesBuilder
 * @see SharedResource
 */
public interface PulsarClientSharedResources extends AutoCloseable {
    enum SharedResource {
        // pulsar-io threadpool
        EventLoopGroup(SharedResourceType.EventLoopGroup),
        // pulsar-external-listener threadpool
        ListenerExecutor(SharedResourceType.ThreadPool),
        // pulsar-timer threadpool
        Timer(SharedResourceType.Timer),
        // pulsar-client-internal threadpool
        InternalExecutor(SharedResourceType.ThreadPool),
        // pulsar-client-scheduled threadpool
        ScheduledExecutor(SharedResourceType.ThreadPool),
        // pulsar-lookup threadpool
        LookupExecutor(SharedResourceType.ThreadPool),
        // DNS resolver and cache that must be shared together with eventLoopGroup
        DnsResolver(SharedResourceType.DnsResolver);

        private final SharedResourceType type;

        SharedResource(SharedResourceType type) {
            this.type = type;
        }

        public SharedResourceType getType() {
            return type;
        }
    }

    enum SharedResourceType {
        EventLoopGroup,
        ThreadPool,
        Timer,
        DnsResolver;
    }

    /**
     * Checks if a resource type is contained in the shared resources instance.
     *
     * @param sharedResource the type of resource to check
     * @return true if the resource type is contained in this instance, false otherwise
     */
    boolean contains(SharedResource sharedResource);

    /**
     * Gets all resource types contained in this shared resources instance.
     *
     * @return set of resource types available in this instance
     */
    Set<SharedResource> getSharedResources();

    /**
     * Creates a new builder for constructing instances of {@link PulsarClientSharedResources}.
     *
     * @return a new {@link PulsarClientSharedResourcesBuilder} which can be used to configure
     *         and build shared resources for use across multiple PulsarClient instances.
     */
    static PulsarClientSharedResourcesBuilder builder() {
        return DefaultImplementation.getDefaultImplementation().newSharedResourcesBuilder();
    }

    /**
     * Closes this instance of PulsarClientSharedResources and releases all associated resources.
     * All PulsarClient instances using these shared resources must be closed before calling this method.
     *
     * @throws PulsarClientException if there is an error while closing the shared resources
     */
    @Override
    void close() throws PulsarClientException;
}
