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

import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;

/**
 * Provides retry-letter / dead-letter producers for a subscription.
 *
 * <p>Implementations control the sharing scope of the underlying producers:
 * <ul>
 *   <li>A non-partitioned {@link ConsumerImpl} is its own provider, holding one producer instance.</li>
 *   <li>A {@link MultiTopicsConsumerImpl} is the provider for all of its partition sub-consumers,
 *       so a single pair of producers is shared across all partitions of the same subscription.</li>
 * </ul>
 *
 * <p>Lifecycle: the producers returned here are owned by the provider. The caller (sub-consumer)
 * MUST NOT close them. The owning provider is responsible for closing them when the subscription
 * itself is being closed.
 */
interface RetryOrDlqProducerProvider {

    /**
     * Get (lazily creating if needed) the shared retry-letter producer for this subscription.
     *
     * @param schemaForSubscription the schema of the owning consumer, used to build an
     *                              AUTO_PRODUCE_BYTES schema when the producer is created for the
     *                              first time.
     */
    CompletableFuture<Producer<byte[]>> getOrCreateRetryLetterProducer(Schema<?> schemaForSubscription);

    /**
     * Get (lazily creating if needed) the shared dead-letter producer for this subscription.
     *
     * @param schemaForSubscription see {@link #getOrCreateRetryLetterProducer}.
     */
    CompletableFuture<Producer<byte[]>> getOrCreateDeadLetterProducer(Schema<?> schemaForSubscription);
}
