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
package org.apache.pulsar.common.migration;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Persistent state of an ongoing metadata store migration.
 */
@Data
@AllArgsConstructor
public class MigrationState {
    /**
     * Current migration phase.
     */
    private MigrationPhase phase;

    /**
     * Target metadata store URL (e.g., "oxia://host:port/namespace").
     */
    private String targetUrl;

    public static final MigrationState NOT_STARTED = new MigrationState(MigrationPhase.NOT_STARTED, null);



    public static final String MIGRATION_FLAG_PATH = "/pulsar/migration-coordinator/migration";
    public static final String PARTICIPANTS_PATH = "/pulsar/migration-coordinator/participants";
}
