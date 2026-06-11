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

/**
 * Pulsar Client API v5.
 *
 * <p>This is a redesigned client API that provides a cleaner, more modern interface for
 * interacting with Apache Pulsar. Key design principles:
 *
 * <ul>
 *   <li><strong>No partition concept</strong> -- topics are opaque; only per-key ordering is
 *       guaranteed when a key is specified.</li>
 *   <li><strong>Streaming vs queuing split</strong> -- {@link org.apache.pulsar.client.api.v5.StreamConsumer}
 *       provides ordered consumption with cumulative acknowledgment, while
 *       {@link org.apache.pulsar.client.api.v5.QueueConsumer} provides parallel unordered
 *       consumption with individual acknowledgment.</li>
 *   <li><strong>Managed subscriptions</strong> -- consumers use broker-managed position tracking
 *       with automatic redelivery and dead-letter support.</li>
 *   <li><strong>Checkpoint consumer</strong> -- {@link org.apache.pulsar.client.api.v5.CheckpointConsumer}
 *       provides unmanaged consumption with consistent {@link org.apache.pulsar.client.api.v5.Checkpoint}
 *       support, designed for connector frameworks (Flink, Spark).</li>
 *   <li><strong>Modern Java</strong> -- uses {@link java.time.Duration}, {@link java.util.Optional},
 *       {@link java.time.Instant}, records, and method-reference-friendly naming.</li>
 *   <li><strong>Transactions</strong> are first-class citizens, integrated directly into the
 *       producer and consumer APIs.</li>
 * </ul>
 *
 * <p>Entry point: {@link org.apache.pulsar.client.api.v5.PulsarClient#builder()}.
 */
package org.apache.pulsar.client.api.v5;
