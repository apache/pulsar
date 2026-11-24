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
package org.apache.pulsar.broker;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.common.api.proto.CommandGetTopicsOfNamespace;
import org.apache.pulsar.common.classification.InterfaceStability;
import org.apache.pulsar.common.naming.NamespaceName;
import org.jspecify.annotations.Nullable;

/**
 * Extended PulsarResources that provides additional functionality beyond PulsarResources.
 *
 * <p>This interface is designed to be pluggable, allowing custom implementations
 * to provide extended PulsarResources capabilities such as custom topic listing strategies</p>
 *
 * <p>Implementations of this interface can be registered with PulsarService to
 * provide extended resource management capabilities.</p>
 */
@InterfaceStability.Evolving
public interface PulsarResourcesExtended {

    /**
     * Lists topics in a namespace with optional property-based filtering.
     *
     * <p>This method provides a flexible way to list topics in a namespace,
     * supporting property-based filtering through the properties parameter.</p>
     *
     * @param namespaceName the namespace to list topics from
     * @param mode the listing mode (ALL, PERSISTENT, NON_PERSISTENT)
     * @param properties optional property filters for topic listing, if null or empty, no filtering is applied
     * @return a CompletableFuture containing the list of topic names
     */
    CompletableFuture<List<String>> listTopicOfNamespace(NamespaceName namespaceName,
                                                         CommandGetTopicsOfNamespace.Mode mode,
                                                         @Nullable Map<String, String> properties);

    /**
     * Initializes this extended resources instance with the PulsarService.
     *
     * <p>This method is called during broker startup to provide the implementation
     * with access to the PulsarService and its dependencies.</p>
     *
     * @param pulsarService the PulsarService instance
     */
    void initialize(PulsarService pulsarService);

    /**
     * Closes this extended resources instance and releases any resources.
     *
     * <p>This method should be called during broker shutdown to clean up
     * any resources held by the implementation.</p>
     */
    void close();
}
