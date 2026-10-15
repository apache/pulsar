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
package org.apache.pulsar.broker.storage;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.pulsar.common.policies.data.EnsemblePlacementPolicyConfig;
import org.apache.pulsar.common.policies.data.EnsemblePlacementPolicyConfig.ParseEnsemblePlacementPolicyConfigException;

/**
 * A borrowed BookKeeper client together with the placement metadata that must be used for new ledgers.
 */
public final class BookKeeperClientContext {
    private final BookKeeper bookKeeper;
    private final byte[] encodedPlacementPolicyConfig;

    private BookKeeperClientContext(BookKeeper bookKeeper, byte[] encodedPlacementPolicyConfig) {
        this.bookKeeper = Objects.requireNonNull(bookKeeper);
        this.encodedPlacementPolicyConfig = encodedPlacementPolicyConfig;
    }

    /**
     * Create a context for a BookKeeper client and placement policy.
     *
     * @param bookKeeper the borrowed BookKeeper client
     * @param placementPolicyConfig the policy used by the client, or {@code null} for the default client
     * @return a client context
     * @throws ParseEnsemblePlacementPolicyConfigException if the policy cannot be encoded
     */
    public static BookKeeperClientContext create(BookKeeper bookKeeper,
            EnsemblePlacementPolicyConfig placementPolicyConfig)
            throws ParseEnsemblePlacementPolicyConfigException {
        byte[] encodedConfig = placementPolicyConfig != null
                && placementPolicyConfig.getPolicyClass() != null
                && placementPolicyConfig.getProperties() != null
                ? placementPolicyConfig.encode() : null;
        return new BookKeeperClientContext(bookKeeper, encodedConfig);
    }

    /**
     * Return the borrowed BookKeeper client. The caller must not close it.
     */
    public BookKeeper getBookKeeper() {
        return bookKeeper;
    }

    /**
     * Add the placement policy metadata required for ledger recovery to the supplied metadata.
     *
     * @param metadata component-specific ledger metadata
     * @return metadata containing the placement policy configuration when one is configured
     */
    public Map<String, byte[]> withPlacementMetadata(Map<String, byte[]> metadata) {
        Objects.requireNonNull(metadata);
        if (encodedPlacementPolicyConfig == null) {
            return metadata;
        }
        Map<String, byte[]> metadataWithPlacementPolicy = new HashMap<>(metadata);
        metadataWithPlacementPolicy.put(
                EnsemblePlacementPolicyConfig.ENSEMBLE_PLACEMENT_POLICY_CONFIG,
                encodedPlacementPolicyConfig.clone());
        return Collections.unmodifiableMap(metadataWithPlacementPolicy);
    }
}
