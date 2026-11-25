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
package org.apache.pulsar.broker.service;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.pulsar.common.policies.data.BacklogQuota.BacklogQuotaType.message_age;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.testcontext.PulsarTestContext;
import org.apache.pulsar.broker.testcontext.SpyConfig.SpyType;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class BrokerServiceBacklogQuotaTest {

    @Test
    public void testPropagatesTimeQuotaFailure() throws Exception {
        try (PulsarTestContext context = PulsarTestContext.builderForNonStartableContext()
                .spyConfigCustomizer(spies -> spies.brokerService(SpyType.SPY))
                .build()) {
            PersistentTopic topic = mock(PersistentTopic.class);
            CompletableFuture<Boolean> timeQuotaExceeded = new CompletableFuture<>();
            when(topic.updateOldPositionInfo()).thenReturn(CompletableFuture.completedFuture(null));
            when(topic.checkTimeBacklogExceeded(false)).thenReturn(timeQuotaExceeded);

            BacklogQuotaManager quotaManager = mock(BacklogQuotaManager.class);
            doReturn(quotaManager).when(context.getBrokerService()).getBacklogQuotaManager();
            var failure = new RejectedExecutionException("Producer connection event loop is shut down");
            doThrow(failure).when(quotaManager).handleExceededBacklogQuota(
                    topic, message_age, context.getConfig().isPreciseTimeBasedBacklogQuotaCheck());

            CompletableFuture<Void> check = context.getBrokerService().checkBacklogQuota(topic);
            assertThat(check).as("quota check while the time quota decision is pending").isNotDone();

            timeQuotaExceeded.complete(true);
            assertThat(check).failsWithin(5, SECONDS)
                    .withThrowableOfType(ExecutionException.class).havingCause().isSameAs(failure);
        }
    }

    @Test
    public void testQuotaCheckReturnsFailedFutureWhenPositionUpdateThrows() throws Exception {
        try (PulsarTestContext context = PulsarTestContext.builderForNonStartableContext().build()) {
            PersistentTopic topic = mock(PersistentTopic.class);
            var failure = new IllegalStateException("Position update failed");
            when(topic.updateOldPositionInfo()).thenThrow(failure);

            CompletableFuture<Void> check = context.getBrokerService().checkBacklogQuota(topic);

            assertThat(check).as("quota check after position update throws")
                    .failsWithin(5, SECONDS)
                    .withThrowableOfType(ExecutionException.class).havingCause().isSameAs(failure);
        }
    }
}
