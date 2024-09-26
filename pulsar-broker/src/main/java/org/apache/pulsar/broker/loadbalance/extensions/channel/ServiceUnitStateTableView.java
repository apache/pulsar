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
package org.apache.pulsar.broker.loadbalance.extensions.channel;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.common.naming.NamespaceBundle;

/**
 * Given that the ServiceUnitStateChannel event-sources service unit (bundle) ownership states via a persistent store
 * and reacts to ownership changes, the ServiceUnitStateTableView provides an interface to the
 * ServiceUnitStateChannel's persistent store and its locally replicated ownership view (tableview) with listener
 * registration. It initially populates its local table view by scanning existing items in the remote store. The
 * ServiceUnitStateTableView receives notifications whenever ownership states are updated in the remote store, and
 * upon notification, it applies the updates to its local tableview with the listener logic.
 */
public interface ServiceUnitStateTableView extends Closeable {

    /**
     * Starts the tableview.
     * It initially populates its local table view by scanning existing items in the remote store, and it starts
     * listening to service unit ownership changes from the remote store.
     * @param pulsar pulsar service reference
     * @param tailItemListener listener to listen tail(newly updated) items
     * @param existingItemListener listener to listen existing items
     * @throws IOException if it fails to init the tableview.
     */
    void start(PulsarService pulsar,
               BiConsumer<String, ServiceUnitStateData> tailItemListener,
               BiConsumer<String, ServiceUnitStateData> existingItemListener) throws IOException;


    /**
     * Closes the tableview.
     * @throws IOException if it fails to close the tableview.
     */
    void close() throws IOException;

    /**
     * Gets one item from the local tableview.
     * @param key the key to get
     * @return value if exists. Otherwise, null.
     */
    ServiceUnitStateData get(String key);

    /**
     * Tries to put the item in the persistent store.
     * If it completes, all peer tableviews (including the local one) will be notified and be eventually consistent
     * with this put value.
     *
     * It ignores put operation if the input value conflicts with the existing one in the persistent store.
     *
     * @param key the key to put
     * @param value the value to put
     * @return a future to track the completion of the operation
     */
    CompletableFuture<Void> put(String key, ServiceUnitStateData value);

    /**
     * Tries to delete the item from the persistent store.
     * All peer tableviews (including the local one) will be notified and be eventually consistent with this deletion.
     *
     * It ignores delete operation if the key is not present in the persistent store.
     *
     * @param key the key to delete
     * @return a future to track the completion of the operation
     */
    CompletableFuture<Void> delete(String key);

    /**
     * Returns the entry set of the items in the local tableview.
     * @return entry set
     */
    Set<Map.Entry<String, ServiceUnitStateData>> entrySet();

    /**
     * Returns service units (namespace bundles) owned by this broker.
     * @return a set of owned service units (namespace bundles)
     */
    Set<NamespaceBundle> ownedServiceUnits();

    /**
     * Tries to flush any batched or buffered updates.
     * @param waitDurationInMillis time to wait until complete.
     * @throws ExecutionException
     * @throws InterruptedException
     * @throws TimeoutException
     */
    void flush(long waitDurationInMillis) throws ExecutionException, InterruptedException, TimeoutException;
}
