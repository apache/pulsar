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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.pulsar.common.policies.data.ManagedLedgerInternalStats.CursorStats;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class SubscriptionCursorStatsCompatibilityTest {

    @Test
    public void testCompleteSnapshotUsesLegacyMethodAndOverriddenGetters() {
        var topic = mock(PersistentTopic.class);
        var subscription = mock(PersistentSubscription.class);
        var cursor = mock(ManagedCursorImpl.class);
        when(topic.getSubscription("sub")).thenReturn(subscription);
        when(subscription.getCursor()).thenReturn(cursor);
        when(topic.getSubscriptionCursorInternalStats("sub", true)).thenCallRealMethod();

        var builtInSnapshot = new CursorStats();
        builtInSnapshot.readPosition = "7:1";
        when(cursor.getCursorStats(true)).thenReturn(builtInSnapshot);
        when(cursor.getCursorStats()).thenReturn(new CursorStats() {
            @Override
            public String getReadPosition() {
                return "12:3";
            }

            @Override
            public String getIndividuallyDeletedMessages() {
                return "custom range";
            }
        });

        CursorStats stats = topic.getSubscriptionCursorInternalStats("sub", true);
        assertThat(stats.readPosition).isEqualTo("12:3");
        assertThat(stats.individuallyDeletedMessages).isEqualTo("custom range");
        verify(cursor, never()).getCursorStats(true);
    }

    @Test
    public void testSummarySnapshotDoesNotInvokeRangeGetter() {
        var topic = mock(PersistentTopic.class);
        var subscription = mock(PersistentSubscription.class);
        var cursor = mock(ManagedCursorImpl.class);
        when(topic.getSubscription("sub")).thenReturn(subscription);
        when(subscription.getCursor()).thenReturn(cursor);
        when(topic.getSubscriptionCursorInternalStats("sub", false)).thenCallRealMethod();

        when(cursor.getCursorStats(false)).thenReturn(new CursorStats() {
            @Override
            public int getTotalNonContiguousDeletedMessagesRange() {
                return 1;
            }

            @Override
            public String getIndividuallyDeletedMessages() {
                throw new AssertionError("The summary snapshot must not access the range string");
            }
        });

        CursorStats stats = topic.getSubscriptionCursorInternalStats("sub", false);
        assertThat(stats.totalNonContiguousDeletedMessagesRange).isEqualTo(1);
        assertThat(stats.individuallyDeletedMessages).isNull();
        verify(cursor, never()).getCursorStats();
    }
}
