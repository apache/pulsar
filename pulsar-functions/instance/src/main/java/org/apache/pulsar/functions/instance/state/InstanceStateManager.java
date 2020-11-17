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

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.annotations.VisibleForTesting;
import java.util.LinkedHashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.functions.api.StateStore;
import org.apache.pulsar.functions.utils.FunctionCommon;

/**
 * The state manager for managing state stores for a running function instance.
 */
@Slf4j
public class InstanceStateManager implements StateManager {

    private final Map<String, StateStore> stores = new LinkedHashMap<>();

    @VisibleForTesting
    boolean isEmpty() {
        return stores.isEmpty();
    }

    @Override
    public void registerStore(StateStore store) {
        final String storeName = store.fqsn();

        checkArgument(!stores.containsKey(storeName),
            String.format("Store %s has already been registered.", storeName));

        stores.put(storeName, store);
    }

    @Override
    public StateStore getStore(String tenant, String namespace, String name) {
        String storeName = FunctionCommon.getFullyQualifiedName(tenant, namespace, name);
        return stores.get(storeName);
    }

    @Override
    public void close() {
        RuntimeException firstException = null;
        for (Map.Entry<String, StateStore> entry : stores.entrySet()) {
            final StateStore store = entry.getValue();
            if (log.isDebugEnabled()) {
                log.debug("Closing store {}", store.fqsn());
            }
            try {
                store.close();
            } catch (RuntimeException e) {
                if (firstException == null) {
                    firstException = e;
                }
                log.error("Failed to close state store {}: ", store.fqsn(), e);
            }
        }
        stores.clear();
        if (null != firstException) {
            throw firstException;
        }
    }
}
