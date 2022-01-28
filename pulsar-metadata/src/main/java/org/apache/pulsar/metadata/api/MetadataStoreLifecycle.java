/**
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

/**
 * Extension of the {@link MetadataStore} interface that supports lifecycle operation methods which might not
 * be supported by all implementations.
 */
public interface MetadataStoreLifecycle {
    /**
     * Initialize the metadata store cluster if needed.
     *
     * For example, if the backend metadata store is a zookeeper cluster and the pulsar cluster is configured to
     * access the zookeeper cluster in the chroot mode, then this method could be used to initialize the root node
     * during pulsar cluster metadata setup.
     *
     * @return a future to track the async request.
     */
    CompletableFuture<Void> initializeCluster();
}
