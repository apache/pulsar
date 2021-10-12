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

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import lombok.Getter;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.FailureDomainImpl;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreException;

public class BrokerResources extends BaseResources<ClusterData> {

    public BrokerResources(MetadataStore store, int operationTimeoutSec) {
        super(store, ClusterData.class, operationTimeoutSec);
    }

    public Set<String> listActiveBrokers() throws MetadataStoreException {
        return new HashSet<>(super.getChildren(BASE_CLUSTERS_PATH));
    }
}
