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
package org.apache.pulsar.client.impl.transaction;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.impl.TransactionMetaStoreHandler;

/**
 * Strategy for locating transaction-coordinator instances and giving the
 * {@link TransactionCoordinatorClientImpl} a {@link TransactionMetaStoreHandler} per coordinator.
 *
 * <p>The discriminator is the per-connection {@code supports_tc_metadata_discovery} feature flag,
 * read once at {@link TransactionCoordinatorClientImpl#startAsync()}:
 * <ul>
 *   <li>{@link AssignTopicTcDiscovery} — the original mechanism: discover coordinators via a
 *       lookup on the {@code transaction_coordinator_assign} partitioned topic. Used against
 *       brokers that don't advertise the flag (legacy/v4 coordinator, or v5 coordinator
 *       disabled).</li>
 *   <li>{@link WatchTcAssignmentsDiscovery} — the metadata-store election mechanism: open one
 *       assignment watch and point each handler at its partition's elected leader broker. Used
 *       against brokers that advertise the flag.</li>
 * </ul>
 *
 * <p>The handler-routing surface ({@code newTransaction} round-robin, {@code commit}/{@code abort}
 * by {@code TxnID.mostSigBits}) is shared and lives in {@link TransactionCoordinatorClientImpl};
 * only coordinator <em>location</em> differs between strategies.
 */
interface TcDiscovery extends AutoCloseable {

    /**
     * Discover the coordinators and create their handlers. Completes when every handler has
     * connected to its coordinator.
     */
    CompletableFuture<Void> start();

    /**
     * @return the handler for coordinator {@code tcId} (= {@code TxnID.mostSigBits}), or
     *     {@code null} if no such coordinator exists in the current assignment.
     */
    TransactionMetaStoreHandler handlerForCoordinator(long tcId);

    /**
     * @return the handler for the next transaction, chosen round-robin across coordinators, or
     *     {@code null} if there are no coordinators available.
     */
    TransactionMetaStoreHandler nextHandler();

    /** @return all current coordinator handlers. Visible for testing. */
    Collection<TransactionMetaStoreHandler> handlers();
}
