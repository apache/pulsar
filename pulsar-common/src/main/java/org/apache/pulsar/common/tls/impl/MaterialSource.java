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
package org.apache.pulsar.common.tls.impl;

/**
 * A source of loaded {@link TlsMaterial} for one {@code TlsPurpose} inside the default
 * {@code FileBasedTlsFactory} (PIP-478). Implementations re-read their backing (files, and — for the
 * server-side {@code BROKER_CLIENT} fold — an authentication plugin's material) on {@link #refresh()},
 * reporting via {@link RefreshOutcome#changed()} whether the material's <em>value</em> changed since the
 * previous refresh, so equal reloads suppress spurious context rebuilds.
 *
 * <p>Not thread-safe on its own; the owning factory serialises access under its per-source monitor.
 */
interface MaterialSource {

    /**
     * Re-read the backing material, reloading only when it changed since the last successful load.
     *
     * @return the current (possibly rebuilt) material together with whether it changed in value
     * @throws Exception if the material could not be loaded (the last-good material is left untouched so
     *                   the next call retries)
     */
    RefreshOutcome refresh() throws Exception;

    /**
     * The result of a {@link #refresh()}: the current material and whether it changed in value since the
     * previous refresh.
     *
     * @param material the current (last-good) material
     * @param changed  {@code true} when the material's value changed (first load counts as a change)
     */
    record RefreshOutcome(TlsMaterial material, boolean changed) {
    }
}
