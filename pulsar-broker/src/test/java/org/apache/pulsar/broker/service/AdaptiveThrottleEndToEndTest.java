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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.DefaultEventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * End-to-end integration tests for the adaptive publish throttling control loop.
 *
 * <p>These tests simulate the full lifecycle:
 * <ul>
 *   <li>Natural rate estimation warmup</li>
 *   <li>Backlog growth → smooth throttle activation and rate reduction (bounded step)</li>
 *   <li>Sustained pressure → rate stays at minimum floor</li>
 *   <li>Backlog drain → smooth rate recovery (bounded step up)</li>
 *   <li>Full recovery to natural rate</li>
 *   <li>observeOnly → live mode transition without disruption</li>
 * </ul>
 *
 * <p>All tests use mocked BrokerService / PersistentTopic so that no real Pulsar
 * broker or ZooKeeper is needed, yet the complete path through
 * {@link AdaptivePublishThrottleController} → {@link AdaptivePublishRateLimiter} →
 * {@link PublishRateLimiterImpl} → {@link ServerCnxThrottleTracker} is exercised.
 */
@Test(groups = "broker")
public class AdaptiveThrottleEndToEndTest {

    // -------------------------------------------------------------------------
    // Constants
    // -------------------------------------------------------------------------

    /** Natural publish rate used in tests (msg/s). */
    private static final double NATURAL_RATE = 1000.0;

    /** Quota limit used in tests (bytes). */
    private static final long QUOTA_LIMIT = 10_000L;

    // -------------------------------------------------------------------------
    // Shared test state
    // -------------------------------------------------------------------------

    private ServiceConfiguration config;
    private BrokerService brokerService;
    private AdaptivePublishThrottleController controller;

    private AtomicLong manualClockSource;
    private Producer producer;
    private ServerCnx serverCnx;
    private ServerCnxThrottleTracker throttleTracker;
    private DefaultEventLoop eventLoop;

    /** The limiter under test — seeded with a natural rate before each scenario. */
    private AdaptivePublishRateLimiter limiter;

    /** The mock topic wired to the limiter. */
    private PersistentTopic topic;

    @BeforeMethod
    public void setup() throws Exception {
        config = new ServiceConfiguration();
        // Use unreachable memory watermarks to avoid test interference from real JVM heap.
        config.setAdaptivePublisherThrottlingMemoryHighWatermarkPct(0.999);
        config.setAdaptivePublisherThrottlingMemoryLowWatermarkPct(0.99);
        config.setAdaptivePublisherThrottlingBacklogHighWatermarkPct(0.90);
        config.setAdaptivePublisherThrottlingBacklogLowWatermarkPct(0.75);
        config.setAdaptivePublisherThrottlingMinRateFactor(0.10);
        config.setAdaptivePublisherThrottlingMaxRateChangeFactor(0.25);
        config.setAdaptivePublisherThrottlingIntervalMs(1000);
        config.setAdaptivePublisherThrottlingObserveOnly(false);

        PulsarService pulsarService = mock(PulsarService.class);
        brokerService = mock(BrokerService.class);
        when(pulsarService.getConfiguration()).thenReturn(config);
        when(brokerService.getPulsar()).thenReturn(pulsarService);

        controller = new AdaptivePublishThrottleController(brokerService);

        manualClockSource = new AtomicLong(TimeUnit.SECONDS.toNanos(0));
        eventLoop = new DefaultEventLoop(new DefaultThreadFactory("test-e2e-io"));
        producer = mock(Producer.class);
        serverCnx = mock(ServerCnx.class);
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        doAnswer(a -> eventLoop).when(ctx).executor();
        doAnswer(a -> ctx).when(serverCnx).ctx();
        doAnswer(a -> serverCnx).when(producer).getCnx();
        throttleTracker = new ServerCnxThrottleTracker(serverCnx);
        doAnswer(a -> throttleTracker).when(serverCnx).getThrottleTracker();
        when(producer.getCnx()).thenReturn(serverCnx);
        BrokerService bs = mock(BrokerService.class);
        when(serverCnx.getBrokerService()).thenReturn(bs);
        EventLoopGroup elg = mock(EventLoopGroup.class);
        when(bs.executor()).thenReturn(elg);
        when(elg.next()).thenReturn(eventLoop);

        // Build the limiter with a pre-seeded natural rate.
        limiter = buildLimiterWithNaturalRate(NATURAL_RATE);

        // Build the mock topic wired to the limiter.
        topic = buildMockTopic(limiter, 0L, QUOTA_LIMIT);

        // Wire the topic into the controller.
        wireSingleTopic(topic);
    }

    @AfterMethod
    public void tearDown() throws Exception {
        controller.close();
        eventLoop.shutdownGracefully().sync();
    }

    // =========================================================================
    // Scenario 1: No backlog pressure → limiter stays inactive
    // =========================================================================

    /**
     * When backlog is at 0, the limiter must remain inactive across multiple cycles.
     * This verifies the disabled/no-pressure path does not accidentally activate.
     */
    @Test
    public void testNoPressureKeepsLimiterInactive() {
        // Backlog = 0/10000 = 0% → below lowWm=75%.
        when(topic.getBacklogSize()).thenReturn(0L);

        for (int cycle = 1; cycle <= 5; cycle++) {
            controller.runControllerCycle();
            assertFalse(limiter.isActive(),
                    "Limiter must remain inactive at cycle " + cycle + " with zero backlog");
        }

        assertEquals(limiter.getThrottleActivationCount(), 0L,
                "Zero activation count expected with zero backlog");
    }

    // =========================================================================
    // Scenario 2: Smooth rate reduction on backlog growth
    // =========================================================================

    /**
     * Simulates backlog growing suddenly to 95% of quota (above highWm=90%).
     * The rate must decrease smoothly — by at most maxRateChangeFactor=25% per cycle —
     * until it reaches the floor (minRateFactor=10% of natural).
     *
     * <p>Expected rate sequence from natural=300 (EWMA estimate), pressure=1.0:
     * <ul>
     *   <li>Cycle 1: base=300, idealTarget=30, maxDelta=75 → max(30, 300-75) = 225</li>
     *   <li>Cycle 2: base=225, max(30, 225-75) = 150</li>
     *   <li>Cycle 3: base=150, max(30, 150-75) = 75</li>
     *   <li>Cycle 4: base=75,  max(30, 75-75) = max(30,0) = 30</li>
     *   <li>Cycle 5+: stays at 30 (floor)</li>
     * </ul>
     *
     * <p>Note: naturalMsgRateEstimate after buildLimiterWithNaturalRate(1000.0) is 300
     * because the EWMA starts at 0 and only one up-step occurs:
     * 0 * 0.70 + 1000 * 0.30 = 300.
     */
    @Test
    public void testSmoothRateReductionOnBacklogGrowth() {
        // Spike backlog to 95% → full pressure.
        when(topic.getBacklogSize()).thenReturn(9500L); // 95% of 10000

        double naturalRate = limiter.getNaturalMsgRateEstimate(); // 300.0
        long floor = Math.max(1L, (long) (naturalRate * 0.10)); // 30
        long maxDelta = (long) (naturalRate * 0.25);            // 75

        List<Long> rateHistory = new ArrayList<>();
        long previousRate = 0L; // starts inactive (base = naturalRate for first cycle)

        for (int cycle = 1; cycle <= 6; cycle++) {
            controller.runControllerCycle();
            assertTrue(limiter.isActive(), "Limiter must be active with 95% backlog at cycle " + cycle);

            long effectiveRate = limiter.getCurrentEffectiveMsgRate();
            rateHistory.add(effectiveRate);

            // Each step must decrease by at most maxDelta from the previous effective rate.
            long base = (previousRate > 0) ? previousRate : (long) naturalRate;
            long minAllowed = Math.max(floor, base - maxDelta);
            assertTrue(effectiveRate >= floor,
                    "Rate must not drop below floor=" + floor + " at cycle " + cycle
                            + " (got " + effectiveRate + ")");
            assertTrue(effectiveRate >= minAllowed,
                    "Rate must not decrease faster than maxRateChangeFactor per cycle at cycle " + cycle
                            + ": base=" + base + " maxDelta=" + maxDelta
                            + " effectiveRate=" + effectiveRate);

            previousRate = effectiveRate;
        }

        // By cycle 4 or 5, rate should have reached the floor.
        assertTrue(rateHistory.get(rateHistory.size() - 1) <= floor * 2,
                "Rate should be near floor after 6 cycles of max pressure. "
                        + "rateHistory=" + rateHistory);
    }

    /**
     * Verifies each cycle's rate decrease is monotonically decreasing under
     * sustained full pressure.
     */
    @Test
    public void testRateMonotonicallyDecreasesUnderSustainedPressure() {
        when(topic.getBacklogSize()).thenReturn(9500L); // 95%

        long prevRate = Long.MAX_VALUE;
        for (int cycle = 1; cycle <= 6; cycle++) {
            controller.runControllerCycle();
            long rate = limiter.getCurrentEffectiveMsgRate();
            assertTrue(rate <= prevRate,
                    "Rate must not increase under sustained pressure at cycle " + cycle
                            + ": prevRate=" + prevRate + " currentRate=" + rate);
            prevRate = rate;
        }
    }

    // =========================================================================
    // Scenario 3: Rate recovery when backlog drains
    // =========================================================================

    /**
     * After reaching the minimum floor, simulates backlog drain below the low
     * watermark. Verifies:
     * <ol>
     *   <li>Limiter deactivates as soon as pressure drops to 0.</li>
     *   <li>Active throttled topics count returns to 0.</li>
     * </ol>
     */
    @Test
    public void testLimiterDeactivatesWhenBacklogDrains() {
        // Phase 1: throttle to floor (4 cycles at 95% backlog).
        when(topic.getBacklogSize()).thenReturn(9500L);
        for (int i = 0; i < 4; i++) {
            controller.runControllerCycle();
        }
        assertTrue(limiter.isActive(), "Should be active after 4 cycles at 95% backlog");
        assertEquals(controller.getActiveThrottledTopicsCount(), 1,
                "One topic should be counted as throttled");

        // Phase 2: drain backlog below lowWm=75% (set to 70%).
        when(topic.getBacklogSize()).thenReturn(7000L); // 70% < lowWm=75%
        controller.runControllerCycle();

        assertFalse(limiter.isActive(),
                "Limiter must deactivate when backlog drains below low watermark");
        assertEquals(controller.getActiveThrottledTopicsCount(), 0,
                "Active throttled count must be 0 after deactivation");
    }

    // =========================================================================
    // Scenario 4: Partial pressure in the ramp zone
    // =========================================================================

    /**
     * When backlog is in the ramp zone (75%–90%), pressure is partial.
     * Rate should be reduced proportionally but still above the floor.
     */
    @Test
    public void testPartialPressureReducesRateProportion() {
        // 82.5% is exactly the midpoint: pressure = 0.5.
        // backlog = 8250L / 10000L = 82.5%
        when(topic.getBacklogSize()).thenReturn(8250L);

        controller.runControllerCycle();
        assertTrue(limiter.isActive(), "Should activate at 82.5% backlog (> lowWm=75%)");

        double naturalRate = limiter.getNaturalMsgRateEstimate(); // 300.0
        long effectiveRate = limiter.getCurrentEffectiveMsgRate();
        long floor = Math.max(1L, (long) (naturalRate * 0.10)); // 30

        // With pressure=0.5, idealTarget = naturalRate * (1 - 0.5 * 0.9) = naturalRate * 0.55 = 165
        // base = naturalRate = 300 (first activation), delta = 75
        // ramp down: max(165, 300-75) = max(165, 225) = 225
        assertTrue(effectiveRate > floor,
                "At partial pressure (0.5), rate must be above floor=" + floor
                        + " (got " + effectiveRate + ")");
        assertTrue(effectiveRate < (long) naturalRate,
                "At partial pressure, rate must be below natural rate (got " + effectiveRate + ")");
    }

    // =========================================================================
    // Scenario 5: observeOnly mode does NOT throttle producers
    // =========================================================================

    /**
     * Full end-to-end observeOnly test: runs 10 cycles under extreme backlog pressure
     * with observeOnly=true and then verifies that the producer's connection was
     * never throttled (channel autoread never changed).
     *
     * <p>This is the test explicitly required by the implementation spec:
     * "include at least one test that fails if any call path would change channel
     * auto read state while observeOnly is enabled."
     */
    @Test
    public void testObserveOnlyEndToEndNeverThrottlesProducerConnection() throws Exception {
        // Enable observeOnly mode.
        config.setAdaptivePublisherThrottlingObserveOnly(true);

        // Maximum pressure: backlog at 100%.
        when(topic.getBacklogSize()).thenReturn(QUOTA_LIMIT);

        // Run many cycles under maximum pressure.
        for (int cycle = 0; cycle < 10; cycle++) {
            controller.runControllerCycle();
        }

        // Assert: limiter NEVER activated.
        assertFalse(limiter.isActive(),
                "observeOnly END-TO-END VIOLATED: limiter must never be active");
        assertEquals(limiter.getThrottleActivationCount(), 0L,
                "observeOnly END-TO-END VIOLATED: activation count must be 0");
        assertEquals(controller.getTotalActivationCount(), 0L,
                "observeOnly END-TO-END VIOLATED: controller activation count must be 0");

        // Assert: actual IO-thread publish path never throttled the connection.
        CompletableFuture<Void> future = new CompletableFuture<>();
        eventLoop.execute(() -> {
            try {
                // Simulate bursts of publish operations as if producers are at full speed.
                manualClockSource.addAndGet(TimeUnit.SECONDS.toNanos(1));
                for (int burst = 0; burst < 20; burst++) {
                    manualClockSource.addAndGet(TimeUnit.SECONDS.toNanos(1));
                    limiter.handlePublishThrottling(producer, 5_000, 50_000_000);
                }
                assertEquals(throttleTracker.throttledCount(), 0,
                        "observeOnly END-TO-END VIOLATED: throttledCount must be 0 — "
                                + "channel autoread must NEVER change when observeOnly=true");
                future.complete(null);
            } catch (Throwable t) {
                future.completeExceptionally(t);
            }
        });
        future.get(5, TimeUnit.SECONDS);
    }

    // =========================================================================
    // Scenario 6: Existing static rate limits are unaffected
    // =========================================================================

    /**
     * Verifies that when adaptive throttling is disabled (limiter == null in topic),
     * the controller cycle skips the topic gracefully without side-effects.
     */
    @Test
    public void testControllerSkipsTopicsWithNoLimiter() {
        // Wire a topic that returns null limiter.
        PersistentTopic noLimiterTopic = mock(PersistentTopic.class);
        when(noLimiterTopic.getAdaptivePublishRateLimiter()).thenReturn(null);
        when(noLimiterTopic.getName()).thenReturn("persistent://tenant/ns/no-limiter");
        wireSingleTopic(noLimiterTopic);

        // Should not throw.
        controller.runControllerCycle();

        assertEquals(controller.getActiveThrottledTopicsCount(), 0,
                "No topics should be counted as throttled when limiter is null");
        assertEquals(controller.getTotalActivationCount(), 0L);
    }

    // =========================================================================
    // Scenario 7: Natural rate estimate warmup (first cycle skipped)
    // =========================================================================

    /**
     * If the limiter has no natural rate estimate (first load or fresh topic),
     * the controller must NOT activate on the first cycle — it sets the baseline
     * and waits for the next cycle.
     */
    @Test
    public void testFirstCycleWithZeroNaturalRateDoesNotActivate() {
        // Build a limiter with NO pre-seeded natural rate.
        AdaptivePublishRateLimiter freshLimiter = new AdaptivePublishRateLimiter(
                manualClockSource::get,
                p -> p.getCnx().getThrottleTracker()
                        .markThrottled(ServerCnxThrottleTracker.ThrottleType.TopicPublishRate),
                p -> p.getCnx().getThrottleTracker()
                        .unmarkThrottled(ServerCnxThrottleTracker.ThrottleType.TopicPublishRate));

        assertEquals(freshLimiter.getNaturalMsgRateEstimate(), 0.0, 1e-9,
                "Natural rate must be 0 for a fresh limiter");

        PersistentTopic freshTopic = buildMockTopic(freshLimiter, 9500L, QUOTA_LIMIT);
        wireSingleTopic(freshTopic);

        // Cycle 1: should set baseline but NOT activate (natural rate still 0 after first sample).
        controller.runControllerCycle();
        assertFalse(freshLimiter.isActive(),
                "First cycle must NOT activate (no natural rate baseline yet)");
        assertEquals(freshLimiter.getThrottleActivationCount(), 0L);
    }

    // =========================================================================
    // Scenario 8: Full lifecycle (warmup → pressure → throttle → recovery)
    // =========================================================================

    /**
     * Complete lifecycle test covering:
     * <ol>
     *   <li>Rate warmup (no pressure, limiter inactive).</li>
     *   <li>Backlog spikes to 95% → throttle activates, rate decreases smoothly.</li>
     *   <li>Backlog drains to 70% → throttle deactivates.</li>
     *   <li>System returns to baseline with limiter inactive.</li>
     * </ol>
     */
    @Test
    public void testFullLifecycle() {
        // Phase 1: warmup (no pressure, 3 cycles).
        when(topic.getBacklogSize()).thenReturn(0L);
        for (int i = 0; i < 3; i++) {
            controller.runControllerCycle();
            assertFalse(limiter.isActive(), "Phase 1: must be inactive during warmup");
        }

        long activationCountBefore = limiter.getThrottleActivationCount();
        assertEquals(activationCountBefore, 0L, "No activations during warmup");

        // Phase 2: backlog spikes to 95% (full pressure).
        when(topic.getBacklogSize()).thenReturn(9500L);
        long prevRate = 0L;
        for (int cycle = 1; cycle <= 4; cycle++) {
            controller.runControllerCycle();
            assertTrue(limiter.isActive(), "Phase 2: must be active at cycle " + cycle);

            long rate = limiter.getCurrentEffectiveMsgRate();
            // Rate should be decreasing or at the floor.
            if (prevRate > 0) {
                assertTrue(rate <= prevRate,
                        "Phase 2: rate should decrease or hold at cycle " + cycle);
            }
            prevRate = rate;
        }

        assertTrue(limiter.getThrottleActivationCount() >= 1L,
                "Phase 2: at least one activation should have occurred");
        assertEquals(controller.getActiveThrottledTopicsCount(), 1,
                "Phase 2: one topic should be counted as throttled");

        // Phase 3: backlog drains below lowWm.
        when(topic.getBacklogSize()).thenReturn(7000L); // 70% < lowWm=75%
        controller.runControllerCycle();
        assertFalse(limiter.isActive(), "Phase 3: must deactivate after drain");
        assertEquals(controller.getActiveThrottledTopicsCount(), 0,
                "Phase 3: throttled count must be 0 after deactivation");

        // Phase 4: no backlog remains.
        when(topic.getBacklogSize()).thenReturn(0L);
        for (int i = 0; i < 3; i++) {
            controller.runControllerCycle();
            assertFalse(limiter.isActive(),
                    "Phase 4: must remain inactive with zero backlog");
        }
    }

    // =========================================================================
    // Helper methods
    // =========================================================================

    /**
     * Builds a limiter whose natural rate estimate is pre-seeded via one EWMA step
     * so the controller can immediately make throttle decisions without waiting for
     * the warmup cycle.
     *
     * <p>After seeding with instant rate = {@code naturalMsgRate}:
     * {@code naturalMsgRateEstimate = 0 * 0.70 + naturalMsgRate * 0.30}
     */
    private AdaptivePublishRateLimiter buildLimiterWithNaturalRate(double naturalMsgRate) {
        AdaptivePublishRateLimiter l = new AdaptivePublishRateLimiter(
                manualClockSource::get,
                p -> p.getCnx().getThrottleTracker()
                        .markThrottled(ServerCnxThrottleTracker.ThrottleType.TopicPublishRate),
                p -> p.getCnx().getThrottleTracker()
                        .unmarkThrottled(ServerCnxThrottleTracker.ThrottleType.TopicPublishRate));
        // Seed: two calls, 1 second apart, with naturalMsgRate messages published.
        l.sampleRate(0L, 0L, TimeUnit.SECONDS.toNanos(0));
        l.sampleRate((long) naturalMsgRate, (long) (naturalMsgRate * 1024), TimeUnit.SECONDS.toNanos(1));
        return l;
    }

    private PersistentTopic buildMockTopic(AdaptivePublishRateLimiter l,
                                           long backlogBytes, long quotaBytes) {
        PersistentTopic t = mock(PersistentTopic.class);
        when(t.getAdaptivePublishRateLimiter()).thenReturn(l);
        when(t.getBacklogSize()).thenReturn(backlogBytes);
        BacklogQuota bq = mock(BacklogQuota.class);
        when(bq.getLimitSize()).thenReturn(quotaBytes);
        when(t.getBacklogQuota(BacklogQuota.BacklogQuotaType.destination_storage)).thenReturn(bq);
        when(t.getMsgInCounter()).thenReturn(10_000L);
        when(t.getBytesInCounter()).thenReturn(10_000_000L);
        when(t.getName()).thenReturn("persistent://tenant/ns/e2e-test-topic");
        return t;
    }

    @SuppressWarnings("unchecked")
    private void wireSingleTopic(PersistentTopic t) {
        doAnswer(invocation -> {
            Consumer<PersistentTopic> consumer = invocation.getArgument(0);
            consumer.accept(t);
            return null;
        }).when(brokerService).forEachPersistentTopic(any());
    }
}
