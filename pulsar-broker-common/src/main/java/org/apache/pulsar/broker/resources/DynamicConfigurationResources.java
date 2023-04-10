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

import com.fasterxml.jackson.core.type.TypeReference;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreException;

public class DynamicConfigurationResources extends BaseResources<Map<String, String>> {

    public static final String BROKER_SERVICE_CONFIGURATION_PATH = "/admin/configuration";

    public DynamicConfigurationResources(MetadataStore store, int operationTimeoutSec) {
        super(store, new TypeReference<Map<String, String>>() {
        }, operationTimeoutSec);
    }

    public CompletableFuture<Optional<Map<String, String>>> getDynamicConfigurationAsync(String path) {
        return getAsync(path);
    }

    public CompletableFuture<Optional<Map<String, String>>> getBrokerDynamicConfigurationAsync(String broker) {
        return getAsync(BROKER_SERVICE_CONFIGURATION_PATH + "/" + broker)
                .thenCombine(getAsync(BROKER_SERVICE_CONFIGURATION_PATH), (brokerData, globalData) -> {
                    Map<String, String> results = new HashMap<>(globalData.orElseGet(HashMap::new));
                    brokerData.ifPresent(results::putAll);
                    return Optional.of(results);
                });

    }


    public Optional<Map<String, String>> getDynamicConfiguration() throws MetadataStoreException {
        return get(BROKER_SERVICE_CONFIGURATION_PATH);
    }

    public void setDynamicConfigurationWithCreate(
                                 Function<Optional<Map<String, String>>, Map<String, String>> createFunction)
            throws MetadataStoreException {
        super.setWithCreate(BROKER_SERVICE_CONFIGURATION_PATH, createFunction);
    }

    public CompletableFuture<Void> setDynamicConfigurationWithCreateAsync(
            Function<Optional<Map<String, String>>, Map<String, String>> createFunction) {
        return super.setWithCreateAsync(BROKER_SERVICE_CONFIGURATION_PATH, createFunction);
    }

    public CompletableFuture<Void> setDynamicConfigurationWithCreateAsync(Set<String> brokers,
                                         Function<Optional<Map<String, String>>, Map<String, String>> createFunction) {

        List<CompletableFuture<Void>> futureList = new ArrayList<>();
        for (String broker: brokers) {
            String path = BROKER_SERVICE_CONFIGURATION_PATH + "/" + broker;
            futureList.add(super.setWithCreateAsync(path, createFunction));
        }
        return FutureUtil.waitForAll(futureList);
    }

    public CompletableFuture<Void> setDynamicConfigurationAsync(
            Function<Map<String, String>, Map<String, String>> updateFunction){
        return super.setAsync(BROKER_SERVICE_CONFIGURATION_PATH, updateFunction);
    }

    public CompletableFuture<Void> setDynamicConfigurationAsync(Set<String> brokers,
            Function<Map<String, String>, Map<String, String>> updateFunction){
        List<CompletableFuture<Void>> futureList = new ArrayList<>();
        for (String broker: brokers) {
            String path = BROKER_SERVICE_CONFIGURATION_PATH + "/" + broker;
            futureList.add(super.setAsync(path, updateFunction));
        }
        return FutureUtil.waitForAll(futureList);
    }

    public void setDynamicConfiguration(
            Function<Map<String, String>, Map<String, String>> updateFunction)
            throws MetadataStoreException {
        super.set(BROKER_SERVICE_CONFIGURATION_PATH, updateFunction);
    }

    public boolean isDynamicConfigurationPath(String path) {
        return BROKER_SERVICE_CONFIGURATION_PATH.equals(path);
    }

    public boolean isDynamicConfigurationPath(String broker, String path) {
        String localBrokerConfigPath = BROKER_SERVICE_CONFIGURATION_PATH + "/" + broker;
        return localBrokerConfigPath.equals(path);
    }
}
