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


import java.util.Optional;

/**
 * Authentication initialization context that provides access to shared services and resources
 * during the authentication provider initialization phase.
 *
 * <p>This context enables authentication implementations to use shared resources such as
 * thread pools, DNS resolvers, and timers that are managed by the Pulsar client, rather than
 * creating their own instances. This improves resource utilization and performance, especially
 * when multiple client instances or authentication providers are used within the same application.
 *
 * <p>Authentication providers should prefer using shared resources when available to minimize
 * system resource consumption and improve performance through better resource reuse.
 *
 * @see Authentication
 */
public interface AuthenticationInitContext {
    /**
     * Retrieves a shared service instance by its class type.
     *
     * <p>This method looks up a globally registered service which is shared among
     * all authentication providers.
     *
     * <p><strong>Example:</strong>
     * <pre>{@code
     * Optional<EventLoopGroup> eventLoop = context.getService(EventLoopGroup.class);
     * if (eventLoop.isPresent()) {
     *     // Use the shared event loop group
     *     this.eventLoopGroup = eventLoop.get();
     * } else {
     *     // Fallback to creating a new instance
     *     this.eventLoopGroup = new NioEventLoopGroup();
     * }
     * }</pre>
     *
     * @param <T> The type of service to retrieve
     * @param serviceClass The class of the service to retrieve
     * @return An {@link Optional} containing the service instance if available,
     *         or {@link Optional#empty()} if no such service is registered
     */
    <T> Optional<T> getService(Class<T> serviceClass);

    /**
     * Retrieves a named shared service instance by its class type and name.
     *
     * <p>This method allows lookup of services that are registered under a specific
     * name, enabling multiple instances of the same service type to be distinguished.
     * This is useful for:
     * <ul>
     *   <li>Specialized DNS resolvers (e.g., "secure-dns", "internal-dns")
     *   <li>Different thread pools for various purposes
     *   <li>Multiple timer instances with different configurations
     *   <li>HTTP clients with different proxy configurations
     * </ul>
     *
     * <p><strong>Example:</strong>
     * <pre>{@code
     * // Get a DNS resolver configured for internal network resolution
     * Optional<NameResolver> internalResolver =
     *     context.getServiceByName(NameResolver.class, "internal-network");
     *
     * // Get a different DNS resolver for external resolution
     * Optional<NameResolver> externalResolver =
     *     context.getServiceByName(NameResolver.class, "external-network");
     * }</pre>
     *
     * @param <T> The type of service to retrieve
     * @param serviceClass The class of the service to retrieve. Cannot be null.
     * @param name The name under which the service is registered. Cannot be null or empty.
     * @return An {@link Optional} containing the named service instance if available,
     *         or {@link Optional#empty()} if no such service is registered with the given name
     */
    <T> Optional<T> getServiceByName(Class<T> serviceClass, String name);
}
