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
package org.apache.pulsar.metadata.api;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Metadata synchronizer to notify and synchronize metadata change events.
 */
public interface MetadataEventSynchronizer {

    /**
     * Notify metadata change event.
     * @param event
     *            metadata change event.
     * @return
     */
    CompletableFuture<Void> notify(MetadataEvent event);

    /**
     * Register notification listener to sync metadata event in local cluster.
     * @param event
     */
    void registerSyncListener(Function<MetadataEvent, CompletableFuture<Void>> event);

    /**
     * Name of current cluster served by the Synchronizer.
     * @return clusterName
     */
    String getClusterName();

    /**
     * close synchronizer resources.
     */
    void close();
}
