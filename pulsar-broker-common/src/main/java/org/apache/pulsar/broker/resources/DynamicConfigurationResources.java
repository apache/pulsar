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

import com.fasterxml.jackson.core.type.TypeReference;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreException;

public class DynamicConfigurationResources extends BaseResources<Map<String, String>> {

    private static final String BROKER_SERVICE_CONFIGURATION_PATH = "/admin/configuration";

    public DynamicConfigurationResources(MetadataStore store, int operationTimeoutSec) {
        super(store, new TypeReference<Map<String, String>>() {
        }, operationTimeoutSec);
    }

    public CompletableFuture<Optional<Map<String, String>>> getDynamicConfigurationAsync() {
        return getAsync(BROKER_SERVICE_CONFIGURATION_PATH);
    }

    public Map<String, String> getDynamicConfiguration() throws MetadataStoreException {
        return get(BROKER_SERVICE_CONFIGURATION_PATH).orElse(Collections.emptyMap());
    }

    public void setDynamicConfigurationWithCreate(
                                 Function<Optional<Map<String, String>>, Map<String, String>> createFunction)
            throws MetadataStoreException {
        super.setWithCreate(BROKER_SERVICE_CONFIGURATION_PATH, createFunction);
    }

    public void setDynamicConfiguration(
            Function<Map<String, String>, Map<String, String>> updateFunction)
            throws MetadataStoreException {
        super.set(BROKER_SERVICE_CONFIGURATION_PATH, updateFunction);
    }

    public boolean isDynamicConfigurationPath(String path) {
        return BROKER_SERVICE_CONFIGURATION_PATH.equals(path);
    }
}
