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

import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.stats.StatsProvider;

/**
 * ManagedLedgerStorageClass represents a configured instance of ManagedLedgerFactory for managed ledgers.
 * This instance is backed by a bookkeeper storage.
 */
public interface BookkeeperManagedLedgerStorageClass extends ManagedLedgerStorageClass {
    /**
     * Return the bookkeeper client instance used by this instance.
     *
     * @return the bookkeeper client.
     */
    BookKeeper getBookKeeperClient();

    /**
     * Return the stats provider to expose the stats of the storage implementation.
     *
     * @return the stats provider.
     */
    StatsProvider getStatsProvider();
}
