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

/**
 * Represents the different phases of metadata store migration.
 */
public enum MigrationPhase {
    /**
     * No migration in progress. Operating normally on source store only.
     */
    NOT_STARTED,

    /**
     * Migration preparation phase. All brokers and bookies are recreating
     * their ephemeral nodes in the target store.
     */
    PREPARATION,

    /**
     * Data copy phase. The migration coordinator is copying persistent
     * data from source to target store.
     */
    COPYING,

    /**
     * Migration completed. All services are using target store. Source
     * store can be decommissioned after configuration update and restart.
     */
    COMPLETED,

    /**
     * Migration has failed. System has rolled-back to used the old metadata store.
     */
    FAILED,
}
