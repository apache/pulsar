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

import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.BrokerServiceException;

public class SystemTopic extends PersistentTopic {

    public SystemTopic(String topic, ManagedLedger ledger, BrokerService brokerService, boolean triggerCompaction)
            throws BrokerServiceException.NamingException, PulsarServerException {
        super(topic, ledger, brokerService);
        if (triggerCompaction && !super.hasCompactionTriggered()) {
            try {
                // To trigger the system topic compaction to avoid lost any data since we are using reader for reading
                // data from the __change_events topic, if no durable subscription on the topic, the data might be lost.
                // Since we are using the topic compaction on the __change_events topic to reduce the topic policy
                // cache recovery time, so we can leverage the topic compaction cursor for retaining the data.

                // We can trigger the compaction directly here since for the first time we create the system topic,
                // The system topic does not have any data, the operation just pre-create the subscription for the
                // topic compaction.
                super.triggerCompaction();
            } catch (BrokerServiceException.AlreadyRunningException ignore) {
                // it's ok here
            }
        }
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
}
