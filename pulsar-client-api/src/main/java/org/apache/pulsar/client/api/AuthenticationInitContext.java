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
 * <p>This context enables authentication implementations to utilize shared resources such as
 * thread pools, DNS resolvers, and timers that are managed by the Pulsar client, rather than
 * creating their own instances. This improves resource utilization and performance, especially
 * when multiple client instances or authentication providers are used within the same application.
 *
 * <h2>Usage Examples</h2>
 *
 * <p><strong>Getting a shared EventLoopGroup:</strong>
 * <pre>{@code
 * public void start(AuthenticationInitContext context) throws PulsarClientException {
 *     Optional<EventLoopGroup> eventLoop = context.getService(EventLoopGroup.class);
 *     if (eventLoop.isPresent()) {
 *         this.sharedEventLoopGroup = eventLoop.get();
 *     } else {
 *         // Fallback to creating own instance
 *         this.ownEventLoopGroup = new NioEventLoopGroup();
 *     }
 * }
 * }</pre>
 *
 * <p><strong>Getting a named DNS resolver:</strong>
 * <pre>{@code
 * public void start(AuthenticationInitContext context) throws PulsarClientException {
 *     Optional<DnsResolver> dnsResolver = context.getServiceByName(DnsResolver.class, "secure-dns");
 *     if (dnsResolver.isPresent()) {
 *         this.dnsResolver = dnsResolver.get();
 *     }
 * }
 * }</pre>
 *
 * <p>Authentication providers should prefer using shared resources when available to minimize
 * system resource consumption and improve performance through better resource reuse.
 *
 * @see Authentication
 * @since 2.11.0
 */
public interface AuthenticationInitContext {
    <T> Optional<T> getService(Class<T> serviceClass);
    <T> Optional<T> getServiceByName(Class<T> serviceClass, String name);
}
