/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.broker.resources;

import java.util.Optional;
import java.util.function.Function;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreException;

public class ClusterHealthStatusResources extends BaseResources<String> {
    public static final String BASE_PATH = "/health-status/";

    public ClusterHealthStatusResources(MetadataStore store, int operationTimeoutSec) {
        super(store, String.class, operationTimeoutSec);
    }

    public void updateHealthStatus(String clusterName, Function<String, String> modifyFunction)
            throws MetadataStoreException {
        set(joinPath(BASE_PATH, clusterName), modifyFunction);
    }

    public Optional<String> getHealthStatus(String clusterName) throws MetadataStoreException {
        return get(joinPath(BASE_PATH, clusterName));
    }

    public enum Status {
        available,
        unavailable
    }
}
