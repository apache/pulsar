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
package org.apache.bookkeeper.mledger.impl;

import com.google.common.collect.ImmutableMap;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.bookkeeper.client.EnsemblePlacementPolicy;
import org.apache.bookkeeper.common.util.JsonUtil.ParseJsonException;
import org.apache.pulsar.common.policies.data.EnsemblePlacementPolicyConfig;

/**
 * Utilities for managing BookKeeper Ledgers custom metadata.
 */
public final class LedgerMetadataUtils {

    private static final String METADATA_PROPERTY_APPLICATION = "application";
    private static final byte[] METADATA_PROPERTY_APPLICATION_PULSAR = "pulsar".getBytes(StandardCharsets.UTF_8);

    private static final String METADATA_PROPERTY_COMPONENT = "component";
    private static final byte[] METADATA_PROPERTY_COMPONENT_MANAGED_LEDGER =
            "managed-ledger".getBytes(StandardCharsets.UTF_8);
    private static final byte[] METADATA_PROPERTY_COMPONENT_COMPACTED_LEDGER =
            "compacted-ledger".getBytes(StandardCharsets.UTF_8);
    private static final byte[] METADATA_PROPERTY_COMPONENT_SCHEMA = "schema".getBytes(StandardCharsets.UTF_8);

    private static final String METADATA_PROPERTY_MANAGED_LEDGER_NAME = "pulsar/managed-ledger";
    private static final String METADATA_PROPERTY_CURSOR_NAME = "pulsar/cursor";
    private static final String METADATA_PROPERTY_COMPACTEDTOPIC = "pulsar/compactedTopic";
    private static final String METADATA_PROPERTY_COMPACTEDTO = "pulsar/compactedTo";
    private static final String METADATA_PROPERTY_SCHEMAID = "pulsar/schemaId";

    /**
     * Build base metadata for every ManagedLedger.
     *
     * @param name the name of the ledger
     * @return an immutable map which describes a ManagedLedger
     */
    static Map<String, byte[]> buildBaseManagedLedgerMetadata(String name) {
        return ImmutableMap.of(
                METADATA_PROPERTY_APPLICATION, METADATA_PROPERTY_APPLICATION_PULSAR,
                METADATA_PROPERTY_COMPONENT, METADATA_PROPERTY_COMPONENT_MANAGED_LEDGER,
                METADATA_PROPERTY_MANAGED_LEDGER_NAME, name.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Build additional metadata for a Cursor.
     *
     * @param name the name of the cursor
     * @return an immutable map which describes the cursor
     * @see #buildBaseManagedLedgerMetadata(java.lang.String)
     */
    static Map<String, byte[]> buildAdditionalMetadataForCursor(String name) {
        return ImmutableMap.of(METADATA_PROPERTY_CURSOR_NAME, name.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Build additional metadata for a CompactedLedger.
     *
     * @param compactedTopic reference to the compacted topic.
     * @param compactedToMessageId last mesasgeId.
     * @return an immutable map which describes the compacted ledger
     */
    public static Map<String, byte[]> buildMetadataForCompactedLedger(String compactedTopic,
                                                                      byte[] compactedToMessageId) {
        return ImmutableMap.of(
                METADATA_PROPERTY_APPLICATION, METADATA_PROPERTY_APPLICATION_PULSAR,
                METADATA_PROPERTY_COMPONENT, METADATA_PROPERTY_COMPONENT_COMPACTED_LEDGER,
                METADATA_PROPERTY_COMPACTEDTOPIC, compactedTopic.getBytes(StandardCharsets.UTF_8),
                METADATA_PROPERTY_COMPACTEDTO, compactedToMessageId
        );
    }

    /**
     * Build additional metadata for a Schema.
     *
     * @param schemaId id of the schema
     * @return an immutable map which describes the schema
     */
    public static Map<String, byte[]> buildMetadataForSchema(String schemaId) {
        return ImmutableMap.of(
                METADATA_PROPERTY_APPLICATION, METADATA_PROPERTY_APPLICATION_PULSAR,
                METADATA_PROPERTY_COMPONENT, METADATA_PROPERTY_COMPONENT_SCHEMA,
                METADATA_PROPERTY_SCHEMAID, schemaId.getBytes(StandardCharsets.UTF_8)
        );
    }

    /**
     * Build additional metadata for the placement policy config.
     *
     * @param className
     *          the ensemble placement policy classname
     * @param properties
     *          the ensemble placement policy properties
     * @return
     *          the additional metadata
     * @throws ParseJsonException
     *          placement policy configuration encode error
     */
    static Map<String, byte[]> buildMetadataForPlacementPolicyConfig(
        Class<? extends EnsemblePlacementPolicy> className, Map<String, Object> properties)
        throws EnsemblePlacementPolicyConfig.ParseEnsemblePlacementPolicyConfigException {
        EnsemblePlacementPolicyConfig config = new EnsemblePlacementPolicyConfig(className, properties);
        return ImmutableMap.of(EnsemblePlacementPolicyConfig.ENSEMBLE_PLACEMENT_POLICY_CONFIG, config.encode());
    }

    private LedgerMetadataUtils() {}

}
