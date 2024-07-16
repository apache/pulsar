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
package org.apache.bookkeeper.mledger;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.bookkeeper.mledger.impl.MetaStore;
import org.apache.bookkeeper.mledger.impl.ReadOnlyManagedLedgerImpl;

public class ReadOnlyManagedLedgerImplWrapper implements ReadOnlyManagedLedger {

    private final ReadOnlyManagedLedgerImpl readOnlyManagedLedger;

    public ReadOnlyManagedLedgerImplWrapper(ManagedLedgerFactoryImpl factory, BookKeeper bookKeeper, MetaStore store,
                                            ManagedLedgerConfig config, OrderedScheduler scheduledExecutor,
                                            String name) {
        this.readOnlyManagedLedger =
                new ReadOnlyManagedLedgerImpl(factory, bookKeeper, store, config, scheduledExecutor, name);
    }

    public CompletableFuture<Void> initialize() {
        return readOnlyManagedLedger.initialize();
    }

    @Override
    public void asyncReadEntry(Position position, AsyncCallbacks.ReadEntryCallback callback, Object ctx) {
        readOnlyManagedLedger.asyncReadEntry(position, callback, ctx);
    }

    @Override
    public long getNumberOfEntries() {
        return readOnlyManagedLedger.getNumberOfEntries();
    }

    @Override
    public ReadOnlyCursor createReadOnlyCursor(Position position) {
        return readOnlyManagedLedger.createReadOnlyCursor(position);
    }

    @Override
    public Map<String, String> getProperties() {
        return readOnlyManagedLedger.getProperties();
    }
}
