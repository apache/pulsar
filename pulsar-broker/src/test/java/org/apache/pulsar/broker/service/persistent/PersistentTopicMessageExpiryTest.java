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

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.util.concurrent.RejectedExecutionException;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.pulsar.broker.testcontext.PulsarTestContext;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class PersistentTopicMessageExpiryTest {

    private static final int TTL_SECONDS = 60;

    private PulsarTestContext context;
    private PersistentTopic topic;

    @BeforeMethod(alwaysRun = true)
    public void setUp() throws Exception {
        context = PulsarTestContext.builderForNonStartableContext().build();
        topic = new PersistentTopic(
                "persistent://public/default/message-expiry",
                context.getBrokerService(),
                mock(ManagedLedger.class),
                mock(MessageDeduplication.class));
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {
        if (context != null) {
            context.close();
            context = null;
        }
    }

    @Test
    public void testSubscriptionFailureDoesNotStopOtherSubscriptions() {
        PersistentSubscription first = mockDurableSubscription("sub-1");
        PersistentSubscription second = mockDurableSubscription("sub-2");
        var failure = new RejectedExecutionException("Topic executor is shut down");

        when(first.expireMessagesAsync(TTL_SECONDS)).thenThrow(failure);
        when(second.expireMessagesAsync(TTL_SECONDS)).thenThrow(failure);
        topic.getSubscriptions().put("sub-1", first);
        topic.getSubscriptions().put("sub-2", second);

        assertThatCode(() -> topic.checkMessageExpiryWithoutSharedPosition(TTL_SECONDS))
                .as("Subscription failures should not escape the expiry check")
                .doesNotThrowAnyException();

        verify(first).expireMessagesAsync(TTL_SECONDS);
        verify(second).expireMessagesAsync(TTL_SECONDS);
    }

    @Test
    public void testReplicatorFailureDoesNotStopOtherReplicators() {
        PersistentReplicator first = mock(PersistentReplicator.class);
        PersistentReplicator second = mock(PersistentReplicator.class);
        var failure = new RejectedExecutionException("Topic executor is shut down");

        when(first.expireMessagesAsync(TTL_SECONDS)).thenThrow(failure);
        when(second.expireMessagesAsync(TTL_SECONDS)).thenThrow(failure);
        topic.getReplicators().put("cluster-1", first);
        topic.getReplicators().put("cluster-2", second);

        assertThatCode(() -> topic.checkMessageExpiryWithoutSharedPosition(TTL_SECONDS))
                .as("Replicator failures should not escape the expiry check")
                .doesNotThrowAnyException();

        verify(first).expireMessagesAsync(TTL_SECONDS);
        verify(second).expireMessagesAsync(TTL_SECONDS);
    }

    @Test
    public void testShadowReplicatorFailureDoesNotStopOtherShadowReplicators() {
        ShadowReplicator first = mock(ShadowReplicator.class);
        ShadowReplicator second = mock(ShadowReplicator.class);
        var failure = new RejectedExecutionException("Topic executor is shut down");

        when(first.expireMessagesAsync(TTL_SECONDS)).thenThrow(failure);
        when(second.expireMessagesAsync(TTL_SECONDS)).thenThrow(failure);
        topic.getShadowReplicators().put("persistent://public/default/shadow-1", first);
        topic.getShadowReplicators().put("persistent://public/default/shadow-2", second);

        assertThatCode(() -> topic.checkMessageExpiryWithoutSharedPosition(TTL_SECONDS))
                .as("Shadow replicator failures should not escape the expiry check")
                .doesNotThrowAnyException();

        verify(first).expireMessagesAsync(TTL_SECONDS);
        verify(second).expireMessagesAsync(TTL_SECONDS);
    }

    private PersistentSubscription mockDurableSubscription(String name) {
        ManagedCursor cursor = mock(ManagedCursor.class);
        when(cursor.isDurable()).thenReturn(true);

        PersistentSubscription subscription = mock(PersistentSubscription.class);
        when(subscription.getName()).thenReturn(name);
        when(subscription.getCursor()).thenReturn(cursor);
        return subscription;
    }
}
