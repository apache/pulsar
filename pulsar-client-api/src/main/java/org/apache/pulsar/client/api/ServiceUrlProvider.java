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

import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;

/**
 * The provider to provide the service url.
 *
 * <p>This allows applications to retrieve the service URL from an external configuration provider and,
 * more importantly, to force the Pulsar client to reconnect if the service URL has been changed.
 *
 * <p>Each provider instance is tied to the lifecycle of one {@link PulsarClient} instance. The client
 * initializes the provider when the client is created and closes the provider when the owning client is
 * closed. Applications that create multiple Pulsar clients should create a separate provider instance
 * for each client instead of sharing one provider.
 *
 * <p>It can be passed with {@link ClientBuilder#serviceUrlProvider(ServiceUrlProvider)}
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface ServiceUrlProvider extends AutoCloseable {

    /**
     * Initialize the service url provider with Pulsar client instance.
     *
     * <p>This can be used by the provider to force the Pulsar client to reconnect whenever the service url might have
     * changed. See {@link PulsarClient#updateServiceUrl(String)}.
     *
     * <p>This method is invoked by the Pulsar client and is expected to be called once for a provider
     * instance. Implementations may reject repeated initialization.
     *
     * @param client
     *            created pulsar client.
     */
    void initialize(PulsarClient client);

    /**
     * Get the current service URL the Pulsar client should connect to.
     *
     * @return the pulsar service url.
     */
    String getServiceUrl();

    /**
     * Close the resource that the provider allocated. The owning Pulsar client invokes this method when
     * it is closed.
     *
     */
    @Override
    default void close() throws Exception {
        // do nothing
    }
}
