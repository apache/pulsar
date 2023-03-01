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
package org.apache.pulsar.broker.resources;

import java.util.Optional;
import lombok.Getter;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;

public class PulsarResources {

    public static final int DEFAULT_OPERATION_TIMEOUT_SEC = 30;

    @Getter
    private final TenantResources tenantResources;
    @Getter
    private final ClusterResources clusterResources;
    @Getter
    private final ResourceGroupResources resourcegroupResources;
    @Getter
    private final NamespaceResources namespaceResources;
    @Getter
    private final DynamicConfigurationResources dynamicConfigResources;
    @Getter
    private final LocalPoliciesResources localPolicies;
    @Getter
    private final LoadManagerReportResources loadReportResources;
    @Getter
    private final BookieResources bookieResources;
    @Getter
    private final TopicResources topicResources;
    @Getter
    private final Optional<MetadataStore> localMetadataStore;
    @Getter
    private final Optional<MetadataStore> configurationMetadataStore;

    public PulsarResources(MetadataStore localMetadataStore, MetadataStore configurationMetadataStore) {
        this(localMetadataStore, configurationMetadataStore, DEFAULT_OPERATION_TIMEOUT_SEC);
    }
    public PulsarResources(MetadataStore localMetadataStore, MetadataStore configurationMetadataStore,
            int operationTimeoutSec) {
        if (configurationMetadataStore != null) {
            tenantResources = new TenantResources(configurationMetadataStore, operationTimeoutSec);
            clusterResources = new ClusterResources(configurationMetadataStore, operationTimeoutSec);
            namespaceResources = new NamespaceResources(localMetadataStore, configurationMetadataStore,
                    operationTimeoutSec);
            resourcegroupResources = new ResourceGroupResources(configurationMetadataStore, operationTimeoutSec);
        } else {
            tenantResources = null;
            clusterResources = null;
            namespaceResources  = null;
            resourcegroupResources = null;
        }

        if (localMetadataStore != null) {
            dynamicConfigResources = new DynamicConfigurationResources(localMetadataStore, operationTimeoutSec);
            localPolicies = new LocalPoliciesResources(localMetadataStore, operationTimeoutSec);
            loadReportResources = new LoadManagerReportResources(localMetadataStore, operationTimeoutSec);
            bookieResources = new BookieResources(localMetadataStore, operationTimeoutSec);
            topicResources = new TopicResources(localMetadataStore);
        } else {
            dynamicConfigResources = null;
            localPolicies = null;
            loadReportResources = null;
            bookieResources = null;
            topicResources = null;
        }

        this.localMetadataStore = Optional.ofNullable(localMetadataStore);
        this.configurationMetadataStore = Optional.ofNullable(configurationMetadataStore);
    }

    public static MetadataStoreExtended createMetadataStore(String serverUrls, int sessionTimeoutMs,
                                                            boolean allowReadOnlyOperations)
            throws MetadataStoreException {
        return MetadataStoreExtended.create(serverUrls, MetadataStoreConfig.builder()
                .sessionTimeoutMillis(sessionTimeoutMs).allowReadOnlyOperations(allowReadOnlyOperations).build());
    }

    public static MetadataStoreExtended createConfigMetadataStore(String serverUrls, int sessionTimeoutMs,
                                                                  boolean allowReadOnlyOperations)
            throws MetadataStoreException {
        return MetadataStoreExtended.create(serverUrls, MetadataStoreConfig.builder()
                .sessionTimeoutMillis(sessionTimeoutMs).allowReadOnlyOperations(allowReadOnlyOperations).build());
    }
}
