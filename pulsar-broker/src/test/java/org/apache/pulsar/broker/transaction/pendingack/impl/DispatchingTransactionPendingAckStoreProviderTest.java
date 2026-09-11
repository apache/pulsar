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
package org.apache.pulsar.broker.transaction.pendingack.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.transaction.pendingack.PendingAckStore;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.testng.annotations.Test;

/**
 * Routing tests for {@link DispatchingTransactionPendingAckStoreProvider}: {@code segment://}
 * subscriptions get {@link MetadataPendingAckStore}; everything else falls through to the legacy
 * {@link MLPendingAckStore}.
 */
public class DispatchingTransactionPendingAckStoreProviderTest {

    @Test
    public void routesSegmentSubscriptionToMetadataStore() throws Exception {
        try (MetadataStoreExtended store = MetadataStoreExtended.create("memory:dispatcher-test",
                MetadataStoreConfig.builder().fsyncEnable(false).build())) {
            PersistentSubscription sub = mockSubscription("segment://public/default/topic/0000-ffff-0", store);
            PendingAckStore pas = new DispatchingTransactionPendingAckStoreProvider()
                    .newPendingAckStore(sub).get();
            assertThat(pas).isInstanceOf(MetadataPendingAckStore.class);
            pas.closeAsync().get();
        }
    }

    @Test
    public void initializedBeforeIsRoutedSegmentToMetadata() throws Exception {
        try (MetadataStoreExtended store = MetadataStoreExtended.create("memory:dispatcher-init",
                MetadataStoreConfig.builder().fsyncEnable(false).build())) {
            PersistentSubscription sub = mockSubscription("segment://public/default/topic/0000-ffff-0", store);
            // MetadataPendingAckStoreProvider returns true unconditionally — verify routing reaches it.
            assertThat(new DispatchingTransactionPendingAckStoreProvider()
                    .checkInitializedBefore(sub).get()).isTrue();
        }
    }

    private static PersistentSubscription mockSubscription(String topicName, MetadataStoreExtended store) {
        PersistentSubscription sub = mock(PersistentSubscription.class, RETURNS_DEEP_STUBS);
        when(sub.getTopicName()).thenReturn(topicName);
        when(sub.getName()).thenReturn("my-sub");
        when(sub.getTopic().getBrokerService().getPulsar().getLocalMetadataStore()).thenReturn(store);
        return sub;
    }
}
