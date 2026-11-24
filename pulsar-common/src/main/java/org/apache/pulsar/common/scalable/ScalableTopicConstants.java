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
package org.apache.pulsar.common.scalable;

/**
 * Constants shared between the broker and the scalable-topic (V5) client.
 */
public final class ScalableTopicConstants {

    private ScalableTopicConstants() {
    }

    /**
     * Producer/consumer metadata key set by the V5 SDK on every per-segment v4
     * producer and consumer it creates. Its presence marks a connection as
     * V5-managed (driven by the scalable-topic surface) rather than a legacy v4
     * client talking to the {@code persistent://} topic directly.
     *
     * <p>PIP-475 uses it during regular-to-scalable migration: the migration
     * pre-check enumerates the producers/consumers attached to the source topic
     * and treats any whose metadata lacks this key as a still-connected legacy v4
     * client, which (absent {@code --force}) blocks the migration. V5 clients,
     * which transition transparently via the lookup session, are excluded.
     */
    public static final String V5_MANAGED_METADATA_KEY = "__pulsar.v5.managed";

    /** Value stored under {@link #V5_MANAGED_METADATA_KEY}. */
    public static final String V5_MANAGED_METADATA_VALUE = "true";
}
