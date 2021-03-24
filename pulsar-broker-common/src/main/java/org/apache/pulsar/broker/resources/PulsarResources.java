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

import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;

import lombok.AccessLevel;
import lombok.Getter;

@Getter(AccessLevel.PUBLIC)
public class PulsarResources {

    public static final int DEFAULT_OPERATION_TIMEOUT_SEC = 30;
    private TenantResources tenatResources;
    private ClusterResources clusterResources;
    private NamespaceResources namespaceResources;
    private DynamicConfigurationResources dynamicConfigResources;
    private LocalPoliciesResources localPolicies;
    private LoadManagerReportResources loadReportResources;
    private Optional<MetadataStoreExtended> localMetadataStore;
    private Optional<MetadataStoreExtended> configurationMetadataStore;

    public PulsarResources(MetadataStoreExtended localMetadataStore, MetadataStoreExtended configurationMetadataStore) {
        this(localMetadataStore, configurationMetadataStore, DEFAULT_OPERATION_TIMEOUT_SEC);
    }
    public PulsarResources(MetadataStoreExtended localMetadataStore, MetadataStoreExtended configurationMetadataStore,
            int operationTimeoutSec) {
        if (configurationMetadataStore != null) {
            tenatResources = new TenantResources(configurationMetadataStore, operationTimeoutSec);
            clusterResources = new ClusterResources(configurationMetadataStore, operationTimeoutSec);
            namespaceResources = new NamespaceResources(configurationMetadataStore, operationTimeoutSec);
        }
        if (localMetadataStore != null) {
            dynamicConfigResources = new DynamicConfigurationResources(localMetadataStore, operationTimeoutSec);
            localPolicies = new LocalPoliciesResources(localMetadataStore, operationTimeoutSec);
            loadReportResources = new LoadManagerReportResources(localMetadataStore, operationTimeoutSec);
        }
        this.localMetadataStore = Optional.ofNullable(localMetadataStore);
        this.configurationMetadataStore = Optional.ofNullable(configurationMetadataStore);
    }

    public static MetadataStoreExtended createMetadataStore(String serverUrls, int sessionTimeoutMs)
            throws MetadataStoreException {
        return MetadataStoreExtended.create(serverUrls, MetadataStoreConfig.builder()
                .sessionTimeoutMillis(sessionTimeoutMs).allowReadOnlyOperations(false).build());
    }
}
