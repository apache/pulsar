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
package org.apache.pulsar.broker.loadbalance.extensions;

import java.io.Closeable;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.loadbalance.extensions.data.BrokerLookupData;
import org.apache.pulsar.broker.namespace.LookupOptions;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.ServiceUnitId;

/**
 * Find the appropriate broker for service unit (e.g. bundle) through different load balancer Implementation.
 */
public interface ExtensibleLoadManager extends Closeable {

    /**
     * Start the extensible load manager.
     *
     * 1. Start the broker registry.
     * 2. Register self to registry.
     * 3. Start the load data store.
     * 4. Init the load manager context.
     * 5. Start load data reporter.
     * 6. Start the namespace unload scheduler.
     * 7. Start the namespace split scheduler.
     * 8. Listen the broker up or down, so we can split immediately.
     */
    void start() throws PulsarServerException;

    /**
     * Initialize this load manager using the given pulsar service.
     */
    void initialize(PulsarService pulsar);

    /**
     * The incoming service unit (e.g. bundle) selects the appropriate broker through strategies.
     *
     * @param topic The optional topic, some method won't provide topic var in this param
     *              (e.g. {@link NamespaceService#internalGetWebServiceUrl(NamespaceBundle, LookupOptions)}),
     *              So the topic is optional.
     * @param serviceUnit service unit (e.g. bundle).
     * @return The broker lookup data.
     */
    CompletableFuture<Optional<BrokerLookupData>> assign(Optional<ServiceUnitId> topic, ServiceUnitId serviceUnit);

    /**
     * Check the incoming service unit is owned by the current broker.
     *
     * @param topic The optional topic, some method won't provide topic var in this param.
     * @param serviceUnit The service unit (e.g. bundle).
     * @return The broker lookup data.
     */
    CompletableFuture<Boolean> checkOwnershipAsync(Optional<ServiceUnitId> topic, ServiceUnitId serviceUnit);

    /**
     * Close the load manager.
     *
     * @throws PulsarServerException if it fails to stop the load manager.
     */
    void close() throws PulsarServerException;
}
