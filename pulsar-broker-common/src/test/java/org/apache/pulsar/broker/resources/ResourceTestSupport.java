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
package org.apache.pulsar.broker.resources;

import com.google.common.io.Closer;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.MetadataStoreFactory;

final class ResourceTestSupport {
    private ResourceTestSupport() {
    }

    static MetadataStore newMemoryStore() throws MetadataStoreException {
        return MetadataStoreFactory.create("memory:local", MetadataStoreConfig.builder().build());
    }

    static NamespaceResources newNamespaceResources(MetadataStore store, Closer caches) {
        NamespaceResources resources = new NamespaceResources(store, 5);
        caches.register(resources.getCache()::close);
        caches.register(resources.getIsolationPolicies().getCache()::close);
        caches.register(resources.getPartitionedTopicResources().getCache()::close);
        return resources;
    }
}
