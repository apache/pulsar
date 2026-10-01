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
package org.apache.pulsar.broker.transaction.buffer.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import io.netty.buffer.ByteBuf;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.transaction.buffer.TransactionBuffer;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.testng.annotations.Test;

/**
 * Routing tests for the default {@link DispatchingTransactionBufferProvider}: {@code segment://}
 * topics get {@link MetadataTransactionBuffer}, everything else falls through to the legacy
 * {@link TopicTransactionBuffer}.
 */
public class DispatchingTransactionBufferProviderTest {

    @Test
    public void routesSegmentTopicToMetadataBuffer() throws Exception {
        try (MetadataStoreExtended store = MetadataStoreExtended.create("memory:dispatcher-test",
                MetadataStoreConfig.builder().fsyncEnable(false).build())) {
            PersistentTopic topic = mockTopic("segment://public/default/topic/0000-ffff-0", store);
            TransactionBuffer tb = new DispatchingTransactionBufferProvider().newTransactionBuffer(topic);
            assertThat(tb).isInstanceOf(MetadataTransactionBuffer.class);
            tb.closeAsync().get();
        }
    }

    @Test
    public void routesPersistentTopicToLegacyBuffer() {
        PersistentTopic topic = mockTopic("persistent://public/default/topic", null);
        TransactionBuffer tb = new DispatchingTransactionBufferProvider().newTransactionBuffer(topic);
        assertThat(tb).isInstanceOf(TopicTransactionBuffer.class);
    }

    private static PersistentTopic mockTopic(String name, MetadataStoreExtended store) {
        PersistentTopic t = mock(PersistentTopic.class, RETURNS_DEEP_STUBS);
        when(t.getName()).thenReturn(name);
        ManagedLedger ml = mock(ManagedLedger.class);
        when(ml.getLastConfirmedEntry()).thenReturn(PositionFactory.create(0, 0));
        doAnswer(inv -> {
            AsyncCallbacks.AddEntryCallback cb = inv.getArgument(1);
            cb.addComplete(PositionFactory.create(0, 0), inv.getArgument(0), inv.getArgument(2));
            return null;
        }).when(ml).asyncAddEntry(any(ByteBuf.class), any(), any());
        when(t.getManagedLedger()).thenReturn(ml);
        if (store != null) {
            when(t.getBrokerService().getPulsar().getLocalMetadataStore()).thenReturn(store);
        }
        return t;
    }
}
