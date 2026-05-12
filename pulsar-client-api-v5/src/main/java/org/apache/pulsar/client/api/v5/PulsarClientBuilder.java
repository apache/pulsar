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
package org.apache.pulsar.client.api.v5;

import io.opentelemetry.api.OpenTelemetry;
import java.time.Duration;
import org.apache.pulsar.client.api.v5.auth.Authentication;
import org.apache.pulsar.client.api.v5.config.ConnectionPolicy;
import org.apache.pulsar.client.api.v5.config.MemorySize;
import org.apache.pulsar.client.api.v5.config.TlsPolicy;
import org.apache.pulsar.client.api.v5.config.TransactionPolicy;

/**
 * Builder for configuring and creating a {@link PulsarClient}.
 */
public interface PulsarClientBuilder {

    /**
     * Build and return the configured client.
     *
     * @return the configured {@link PulsarClient} instance
     * @throws PulsarClientException if the client cannot be created (e.g., invalid configuration
     *         or connection failure)
     */
    PulsarClient build() throws PulsarClientException;

    /**
     * Set the Pulsar service URL — the broker's binary-protocol endpoint.
     *
     * <p>Must use {@code pulsar://} or {@code pulsar+ssl://}. The admin/web
     * service URL ({@code http://...} / {@code https://...}) is NOT accepted.
     *
     * @param serviceUrl the Pulsar broker service URL to connect to,
     *                   e.g. {@code pulsar://localhost:6650}
     * @return this builder instance for chaining
     * @throws IllegalArgumentException if {@code serviceUrl} is null, blank, or
     *         does not use {@code pulsar://} / {@code pulsar+ssl://}
     */
    PulsarClientBuilder serviceUrl(String serviceUrl);

    /**
     * Set the authentication provider.
     *
     * @param authentication the authentication provider to use for connecting to the broker
     * @return this builder instance for chaining
     */
    PulsarClientBuilder authentication(Authentication authentication);

    /**
     * Set authentication by plugin class name and parameter string.
     *
     * @param authPluginClassName the fully qualified class name of the authentication plugin
     * @param authParamsString the authentication parameters as a serialized string
     * @return this builder instance for chaining
     * @throws PulsarClientException if the authentication plugin cannot be loaded or configured
     */
    PulsarClientBuilder authentication(String authPluginClassName, String authParamsString)
            throws PulsarClientException;

    /**
     * Timeout for client operations (e.g., creating producers/consumers).
     *
     * @param timeout the maximum duration to wait for an operation to complete
     * @return this builder instance for chaining
     */
    PulsarClientBuilder operationTimeout(Duration timeout);

    /**
     * Configure connection-level settings such as timeouts, pool size, threading,
     * keep-alive, and proxy configuration.
     *
     * @param policy the connection policy
     * @return this builder instance for chaining
     * @see ConnectionPolicy#builder()
     */
    PulsarClientBuilder connectionPolicy(ConnectionPolicy policy);

    /**
     * Set the transaction policy.
     *
     * @param policy the transaction policy controlling transaction behavior and timeouts
     * @return this builder instance for chaining
     */
    PulsarClientBuilder transactionPolicy(TransactionPolicy policy);

    /**
     * Configure TLS for the client connection.
     *
     * @param policy the TLS policy to apply to broker connections
     * @return this builder instance for chaining
     * @see TlsPolicy#of(String)
     * @see TlsPolicy#ofMutualTls(String, String, String)
     * @see TlsPolicy#ofInsecure()
     */
    PulsarClientBuilder tlsPolicy(TlsPolicy policy);

    /**
     * Provide a custom {@link OpenTelemetry} instance for metrics and tracing.
     *
     * <p>If not set, the client creates its own internal instance that exports metrics
     * (via a Prometheus-compatible endpoint) with tracing disabled.
     *
     * <p>When a custom instance is provided, the client uses whatever {@code MeterProvider}
     * and {@code TracerProvider} it contains. This means:
     * <ul>
     *   <li>To keep metrics only (no tracing), configure the instance with a
     *       {@code MeterProvider} and leave the {@code TracerProvider} as no-op.</li>
     *   <li>To enable distributed tracing, configure the instance with both a
     *       {@code MeterProvider} and a {@code TracerProvider}.</li>
     *   <li>To disable all telemetry, pass {@link OpenTelemetry#noop()}.</li>
     * </ul>
     *
     * @param openTelemetry the OpenTelemetry instance to use
     * @return this builder instance for chaining
     */
    PulsarClientBuilder openTelemetry(OpenTelemetry openTelemetry);

    /**
     * Maximum amount of direct memory the client can use for pending messages.
     *
     * @param size the memory limit for pending messages across all producers
     * @return this builder instance for chaining
     * @see MemorySize#ofMegabytes(long)
     * @see MemorySize#ofGigabytes(long)
     */
    PulsarClientBuilder memoryLimit(MemorySize size);

    // --- Misc ---

    /**
     * Set the listener name for multi-listener brokers.
     *
     * @param name the listener name to use when connecting to brokers that advertise
     *        multiple listener endpoints
     * @return this builder instance for chaining
     */
    PulsarClientBuilder listenerName(String name);

    /**
     * A human-readable description of this client (for logging and debugging).
     *
     * @param description a descriptive label for this client instance
     * @return this builder instance for chaining
     */
    PulsarClientBuilder description(String description);
}
