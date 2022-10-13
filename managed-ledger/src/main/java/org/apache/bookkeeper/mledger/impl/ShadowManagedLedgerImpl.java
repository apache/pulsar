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

import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.pulsar.common.naming.TopicName;

/**
 * Working in progress until <a href="https://github.com/apache/pulsar/issues/16153">PIP-180</a> is finished.
 * Currently, it works nothing different with ManagedLedgerImpl.
 */
@Slf4j
public class ShadowManagedLedgerImpl extends ManagedLedgerImpl {

    private final TopicName shadowSource;
    private final String sourceMLName;

    public ShadowManagedLedgerImpl(ManagedLedgerFactoryImpl factory, BookKeeper bookKeeper,
                                   MetaStore store, ManagedLedgerConfig config,
                                   OrderedScheduler scheduledExecutor,
                                   String name, final Supplier<Boolean> mlOwnershipChecker) {
        super(factory, bookKeeper, store, config, scheduledExecutor, name, mlOwnershipChecker);
        this.shadowSource = TopicName.get(config.getShadowSource());
        this.sourceMLName = shadowSource.getPersistenceNamingEncoding();
    }

    @Override
    synchronized void initialize(ManagedLedgerInitializeLedgerCallback callback, Object ctx) {
        // TODO: ShadowManagedLedger has different initialize process from normal ManagedLedger,
        //  which is complicated and will be implemented in the next PRs.
        super.initialize(callback, ctx);
    }

    public TopicName getShadowSource() {
        return shadowSource;
    }
}
