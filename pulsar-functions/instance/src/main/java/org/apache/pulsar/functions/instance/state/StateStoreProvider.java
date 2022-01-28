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
package org.apache.pulsar.functions.instance.state;

import java.util.Map;
import org.apache.pulsar.functions.api.StateStore;
import org.apache.pulsar.functions.proto.Function.FunctionDetails;

/**
 * The State Store Provider provides the state stores for a function.
 */
public interface StateStoreProvider extends AutoCloseable {

    String STATE_STORAGE_SERVICE_URL = "stateStorageServiceUrl";

    /**
     * The state store provider returns `null` state stores.
     */
    StateStoreProvider NULL = new StateStoreProvider() {
        @Override
        public <S extends StateStore> S getStateStore(String tenant, String namespace, String name) {
            return null;
        }

        @Override
        public void close() {
        }

    };

    /**
     * Initialize the state store provider.
     *
     * @param config the config to init the state store provider.
     * @param functionDetails the function details.
     * @throws Exception when failed to init the state store provider.
     */
    default void init(Map<String, Object> config, FunctionDetails functionDetails) throws Exception {}

    /**
     * Get the state store with the provided store name.
     *
     * @param tenant the tenant that owns this state store
     * @param namespace the namespace that owns this state store
     * @param name the state store name
     * @param <S> the type of interface of the store to return
     * @return the state store instance.
     *
     * @throws ClassCastException if the return type isn't a type
     * or interface of the actual returned store.
     */
    <S extends StateStore> S getStateStore(String tenant, String namespace, String name) throws Exception;

    @Override
    void close();
}
