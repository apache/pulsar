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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.util.Futures;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.ConsumedLedgersCheckSchedulerTest.ManualScheduler;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.testcontext.PulsarTestContext;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.util.SingleThreadNonConcurrentFixedRateScheduler;
import org.mockito.InOrder;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker", timeOut = 30000)
public class BrokerServiceConsumedLedgersMonitorTest {
    private PulsarTestContext context;
    private BrokerService broker;
    private ServiceConfiguration config;
    private ManualScheduler scheduler;
    private List<String> submissions;

    @BeforeMethod
    public void setup() throws Exception {
        config = new ServiceConfiguration();
        config.setClusterName("test");
        config.setBrokerShutdownTimeoutMs(0L);
        config.setLoadBalancerOverrideBrokerNicSpeedGbps(Optional.of(1.0));
        config.setTopicLevelPoliciesEnabled(false);
        config.setSystemTopicEnabled(false);
        context = PulsarTestContext.builderForNonStartableContext().config(config).spyByDefault().build();
        broker = context.getBrokerService();
        scheduler = new ManualScheduler();
        doReturn(scheduler.executor()).when(broker).getConsumedLedgersMonitor();
        submissions = new ArrayList<>();
    }

    @AfterMethod(alwaysRun = true)
    public void cleanup() throws Exception {
        if (context != null) {
            // The topic cache contains mocks whose maintenance submissions are controlled by these tests.
            broker.getTopics().clear();
            context.close();
        }
    }

    @Test
    public void testFirstMaintenancePassDoesNotSubmitEveryTopic() throws Exception {
        doCallRealMethod().when(broker).getConsumedLedgersMonitor();
        SingleThreadNonConcurrentFixedRateScheduler monitor = broker.getConsumedLedgersMonitor();
        // Drain the executor's initialization task before taking control of its first maintenance timer.
        monitor.submit(() -> { }).get(10, TimeUnit.SECONDS);
        for (int i = 0; i < 25; i++) {
            addTopic("first-pass-" + i);
        }
        broker.startConsumedLedgersMonitor();
        Runnable firstCheck = monitor.getQueue().element();
        assertThat(monitor.remove(firstCheck)).isTrue();
        CountDownLatch workerStarted = new CountDownLatch(1);
        CountDownLatch releaseWorker = new CountDownLatch(1);
        var blocker = monitor.submit(() -> {
            workerStarted.countDown();
            releaseWorker.await();
            return null;
        });
        try {
            assertThat(workerStarted.await(10, TimeUnit.SECONDS)).isTrue();
            firstCheck.run();
            assertThat(submissions).hasSize(2);
            assertThat(monitor.getQueue()).hasSize(1);
        } finally {
            releaseWorker.countDown();
            blocker.get(10, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testMonitorSubmitsOneTopicEveryTwentyMilliseconds() {
        List<ManagedLedger> ledgers = new ArrayList<>();
        for (int i = 0; i < 25; i++) {
            ledgers.add(addTopic("paced-" + i));
        }
        broker.startConsumedLedgersMonitor();
        assertThat(submissions).isEmpty();
        assertThat(scheduler.nextDelayNanos()).isEqualTo(Duration.ofSeconds(120).toNanos());

        for (int i = 1; i <= 25; i++) {
            scheduler.runNext();
            assertThat(submissions).hasSize(i * 2);
            if (i < 25) {
                assertThat(scheduler.nextDelayNanos()).isEqualTo(Duration.ofMillis(20).toNanos());
            }
        }
        for (ManagedLedger ledger : ledgers) {
            InOrder order = inOrder(ledger);
            order.verify(ledger).trimConsumedLedgersInBackground(Futures.NULL_PROMISE);
            order.verify(ledger).rolloverCursorsInBackground();
            order.verifyNoMoreInteractions();
        }
        assertThat(scheduler.pendingCount()).isEqualTo(1);
    }

    @DataProvider
    public Object[][] disabledIntervals() {
        return new Object[][] {{0}, {-1}};
    }

    @Test(dataProvider = "disabledIntervals")
    public void testNonPositiveIntervalDisablesTheMonitor(int interval) {
        config.setRetentionCheckIntervalInSeconds(interval);
        addTopic("disabled");
        broker.startConsumedLedgersMonitor();
        verifyNoInteractions(scheduler.executor());
        assertThat(submissions).isEmpty();
    }

    @Test
    public void testMonitorUsesTheConfiguredIntervalAndStartsOnce() {
        config.setRetentionCheckIntervalInSeconds(3);
        for (int i = 0; i < 11; i++) {
            addTopic("custom-interval-" + i);
        }
        broker.startConsumedLedgersMonitor();
        broker.startConsumedLedgersMonitor();
        assertThat(scheduler.pendingCount()).isEqualTo(1);
        assertThat(scheduler.nextDelayNanos()).isEqualTo(Duration.ofSeconds(3).toNanos());
        scheduler.runNext();
        assertThat(submissions).hasSize(2);
        assertThat(scheduler.nextDelayNanos()).isEqualTo(Duration.ofMillis(20).toNanos());
    }

    @Test
    public void testBrokerCloseStopsAnAlreadyDequeuedCheck() throws Exception {
        for (int i = 0; i < 25; i++) {
            addTopic("close-" + i);
        }
        broker.startConsumedLedgersMonitor();
        scheduler.runNext();
        Runnable dequeued = scheduler.takeNext();
        assertThat(submissions).hasSize(2);

        broker.getTopics().clear();
        broker.closeAsync().get(10, TimeUnit.SECONDS);
        dequeued.run();
        assertThat(submissions).hasSize(2);
        assertThat(scheduler.pendingCount()).isZero();
    }

    @Test
    public void testBundleUnloadRemovesPendingTopicFromTheRound() {
        addTopic("unload-a");
        addTopic("unload-b");
        broker.startConsumedLedgersMonitor();
        scheduler.runNext();
        String pendingName = broker.getTopics().keySet().stream()
                .filter(name -> !submissions.contains(name + ":trim")).findFirst().orElseThrow();
        CompletableFuture<Optional<Topic>> expected = broker.getTopics().get(pendingName);
        PersistentTopic pending = (PersistentTopic) expected.join().orElseThrow();
        ManagedLedger unloadedLedger = pending.getManagedLedger();
        when(pending.isFenced()).thenReturn(true);
        NamespaceBundle bundle = mock(NamespaceBundle.class);
        when(bundle.toString()).thenReturn("public/default/0x00000000_0xffffffff");
        broker.cleanUnloadedTopicFromCache(bundle, Map.of(pendingName, expected));
        assertThat(broker.getTopics()).doesNotContainKey(pendingName);

        scheduler.runNext();
        assertThat(submissions).hasSize(2);
        verifyNoInteractions(unloadedLedger);

        ManagedLedger reloaded = addTopic(pendingName.substring(pendingName.lastIndexOf('/') + 1));
        scheduler.runNext();
        scheduler.runNext();
        InOrder order = inOrder(reloaded);
        order.verify(reloaded).trimConsumedLedgersInBackground(Futures.NULL_PROMISE);
        order.verify(reloaded).rolloverCursorsInBackground();
        order.verifyNoMoreInteractions();
    }

    private ManagedLedger addTopic(String suffix) {
        String name = "persistent://public/default/" + suffix;
        PersistentTopic topic = mock(PersistentTopic.class);
        ManagedLedger ledger = mock(ManagedLedger.class);
        when(topic.getManagedLedger()).thenReturn(ledger);
        doAnswer(invocation -> {
            submissions.add(name + ":trim");
            return null;
        }).when(ledger).trimConsumedLedgersInBackground(Futures.NULL_PROMISE);
        doAnswer(invocation -> {
            submissions.add(name + ":rollover");
            return null;
        }).when(ledger).rolloverCursorsInBackground();
        broker.getTopics().put(name, CompletableFuture.completedFuture(Optional.of(topic)));
        return ledger;
    }
}
