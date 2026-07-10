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
package org.apache.pulsar.client.api.v5.auth;

import java.io.Closeable;
import org.apache.pulsar.client.api.v5.PulsarClientException;

/**
 * Pluggable authentication provider for Pulsar clients.
 *
 * <p>Implementations must be thread-safe.
 */
public interface Authentication extends Closeable {

    /**
     * The authentication method name (e.g., "token", "tls").
     *
     * @return the authentication method identifier string
     */
    String authMethodName();

    /**
     * Get the authentication data to be sent to the broker.
     *
     * @return the authentication data containing credentials for the broker
     * @throws PulsarClientException if the authentication data could not be obtained
     */
    AuthenticationData authData() throws PulsarClientException;

    /**
     * Get the authentication data for a specific broker host.
     *
     * <p>The default implementation delegates to {@link #authData()}.
     *
     * @param brokerHostName the hostname of the broker to authenticate against
     * @return the authentication data containing credentials for the specified broker
     * @throws PulsarClientException if the authentication data could not be obtained
     */
    default AuthenticationData authData(String brokerHostName) throws PulsarClientException {
        return authData();
    }

    /**
     * Initialize the authentication provider. Called once when the client is created.
     *
     * @throws PulsarClientException if initialization fails
     */
    default void initialize() throws PulsarClientException {
    }

    @Override
    default void close() {
    }
}
