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
package org.apache.pulsar.broker.admin.impl;

import java.util.HashSet;
import java.util.Set;
import lombok.Getter;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.FailureDomain;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;

public class ClusterResources extends BaseResources<ClusterData> {

    public static final String CLUSTERS_ROOT = "/admin/clusters";
    @Getter
    private FailureDomainResources failureDomainResources;

    public ClusterResources(MetadataStoreExtended store) {
        super(store, ClusterData.class);
        this.failureDomainResources = new FailureDomainResources(store, FailureDomain.class);
    }

    public Set<String> list() throws MetadataStoreException {
        return new HashSet<>(super.getChildren(CLUSTERS_ROOT));
    }

    public static class FailureDomainResources extends BaseResources<FailureDomain> {
        public static final String FAILURE_DOMAIN = "failureDomain";

        public FailureDomainResources(MetadataStoreExtended store, Class<FailureDomain> clazz) {
            super(store, clazz);
        }
    }
}
