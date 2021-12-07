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

package org.apache.pulsar.broker.service.persistent;

import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.service.BrokerService;

public class SystemTopic extends PersistentTopic {

    public SystemTopic(String topic, ManagedLedger ledger, BrokerService brokerService) throws PulsarServerException {
        super(topic, ledger, brokerService);
    }

    @Override
    public boolean isSizeBacklogExceeded() {
        return false;
    }

    @Override
    public boolean isTimeBacklogExceeded() {
        return false;
    }

    @Override
    public boolean isSystemTopic() {
        return true;
    }

    @Override
    public void checkMessageExpiry() {
        // do nothing for system topic
    }

    @Override
    public void checkGC() {
        // do nothing for system topic
    }

    @Override
    public CompletableFuture<Void> checkReplication() {
        return CompletableFuture.completedFuture(null);
    }

    public CompletableFuture<Boolean> isCompactionEnabled() {
        // All system topics are using compaction, even though is not explicitly set in the policies.
        return CompletableFuture.completedFuture(true);
    }
}
