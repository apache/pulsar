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
package org.apache.pulsar.broker.service.persistent;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.DisabledPublishRateLimiter;
import org.apache.pulsar.broker.service.PublishRateLimiter;
import org.apache.pulsar.broker.service.plugin.EntryFilter;
import org.apache.pulsar.common.naming.SystemTopicNames;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.EntryFilters;
import org.apache.pulsar.common.policies.data.Policies;

public class SystemTopic extends PersistentTopic {

    public SystemTopic(String topic, ManagedLedger ledger, BrokerService brokerService) throws PulsarServerException {
        super(topic, ledger, brokerService);
    }

    @Override
    public boolean isDeleteWhileInactive() {
        return false;
    }

    @Override
    public boolean isSizeBacklogExceeded() {
        return false;
    }

    @Override
    public CompletableFuture<Boolean> checkTimeBacklogExceeded(boolean shouldUpdateOldPositionInfo) {
        return CompletableFuture.completedFuture(false);
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
        if (SystemTopicNames.isTopicPoliciesSystemTopic(topic)) {
            return super.checkReplication();
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public boolean isCompactionEnabled() {
        // All system topics are using compaction except `HealthCheck`,
        // even though is not explicitly set in the policies.
        return !NamespaceService.isHeartbeatNamespace(TopicName.get(topic));
    }

    @Override
    public boolean isDeduplicationEnabled() {
        /*
            Disable deduplication on system topic to avoid recovering deduplication WAL
            (especially from offloaded topic).
            Because the system topic usually is a precondition of other topics. therefore,
            we should pay attention on topic loading time.

            Note: If the system topic loading timeout may cause dependent topics to fail to run.

            Dependency diagram: normal topic --rely on--> system topic --rely on--> deduplication recover
                                --may rely on--> (tiered storage)
         */
        return false;
    }

    @Override
    public boolean isEncryptionRequired() {
        // System topics are only written by the broker that can't know the encryption context.
        return false;
    }

    @Override
    public EntryFilters getEntryFiltersPolicy() {
        return null;
    }

    @Override
    public List<EntryFilter> getEntryFilters() {
        return null;
    }

    @Override
    public PublishRateLimiter getBrokerPublishRateLimiter() {
        return DisabledPublishRateLimiter.INSTANCE;
    }

    @Override
    public void updateResourceGroupLimiter(@Nonnull Policies namespacePolicies) {
        // nothing todo.
    }

    @Override
    public Optional<DispatchRateLimiter> getBrokerDispatchRateLimiter() {
        return Optional.empty();
    }
}
