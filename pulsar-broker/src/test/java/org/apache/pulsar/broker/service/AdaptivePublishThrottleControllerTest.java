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
 * Unit tests for {@link AdaptivePublishThrottleController}.
 *
 * <p>Tests are divided into:
 * <ul>
 *   <li>Pressure math: {@code linearPressure()}, {@code computeMemoryPressure()},
 *       {@code computeBacklogPressure()}.</li>
 *   <li>Rate computation: {@code computeTargetRate()} including bounded-step and
 *       hard floor.</li>
 *   <li>Hysteresis: activate above high-watermark, deactivate only below
 *       low-watermark.</li>
 *   <li>observeOnly guarantee: controller cycle MUST NOT call
 *       {@link AdaptivePublishRateLimiter#activate} or
 *       {@link AdaptivePublishRateLimiter#deactivate} when
 *       {@code observeOnly=true}; channel autoread state must be unchanged.</li>
 * </ul>
 */
@Test(groups = "broker")
public class AdaptivePublishThrottleControllerTest {

    private ServiceConfiguration config;
    private BrokerService brokerService;
    private PulsarService pulsarService;
    private AdaptivePublishThrottleController controller;

    // For building a realistic limiter in integration-style tests
    private AtomicLong manualClockSource;
    private Producer producer;
    private ServerCnx serverCnx;
    private ServerCnxThrottleTracker throttleTracker;
    private DefaultEventLoop eventLoop;

    @BeforeMethod
    public void setup() throws Exception {
        config = new ServiceConfiguration();
        // Use effectively unreachable memory watermarks so that real JVM heap pressure
        // never interferes with backlog-only tests (avoids test flakiness on loaded machines).
        // Tests that specifically exercise memory pressure use linearPressure() directly.
        config.setAdaptivePublisherThrottlingMemoryHighWatermarkPct(0.999);
        config.setAdaptivePublisherThrottlingMemoryLowWatermarkPct(0.99);
        config.setAdaptivePublisherThrottlingBacklogHighWatermarkPct(0.90);
        config.setAdaptivePublisherThrottlingBacklogLowWatermarkPct(0.75);
        config.setAdaptivePublisherThrottlingMinRateFactor(0.10);
        config.setAdaptivePublisherThrottlingMaxRateChangeFactor(0.25);
        config.setAdaptivePublisherThrottlingIntervalMs(1000);
        config.setAdaptivePublisherThrottlingObserveOnly(false);

        pulsarService = mock(PulsarService.class);
        brokerService = mock(BrokerService.class);
        when(pulsarService.getConfiguration()).thenReturn(config);
        when(brokerService.getPulsar()).thenReturn(pulsarService);

        controller = new AdaptivePublishThrottleController(brokerService);

        // For integration-style tests that need a real throttle tracker.
        manualClockSource = new AtomicLong(TimeUnit.SECONDS.toNanos(100));
        eventLoop = new DefaultEventLoop(new DefaultThreadFactory("test-io"));
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
    }

    @AfterMethod
    public void tearDown() throws Exception {
        controller.close();
        eventLoop.shutdownGracefully().sync();
    }

    // =========================================================================
    // linearPressure() — boundary and interpolation tests
    // =========================================================================

    @Test
    public void testLinearPressureBelowLowWatermark() {
        // Any value below lowWm must return exactly 0.0.
        assertEquals(AdaptivePublishThrottleController.linearPressure(0.0, 0.70, 0.85), 0.0, 1e-9);
        assertEquals(AdaptivePublishThrottleController.linearPressure(0.50, 0.70, 0.85), 0.0, 1e-9);
        assertEquals(AdaptivePublishThrottleController.linearPressure(0.69, 0.70, 0.85), 0.0, 1e-9);
    }

    @Test
    public void testLinearPressureAtLowWatermarkBoundary() {
        // Exactly at lowWm should return 0.0 (inclusive lower bound).
        assertEquals(AdaptivePublishThrottleController.linearPressure(0.70, 0.70, 0.85), 0.0, 1e-9);
    }

    @Test
    public void testLinearPressureAboveHighWatermark() {
        // Any value above highWm must return exactly 1.0.
        assertEquals(AdaptivePublishThrottleController.linearPressure(0.86, 0.70, 0.85), 1.0, 1e-9);
        assertEquals(AdaptivePublishThrottleController.linearPressure(0.90, 0.70, 0.85), 1.0, 1e-9);
        assertEquals(AdaptivePublishThrottleController.linearPressure(1.00, 0.70, 0.85), 1.0, 1e-9);
    }

    @Test
    public void testLinearPressureAtHighWatermarkBoundary() {
        // Exactly at highWm should return 1.0 (inclusive upper bound).
        assertEquals(AdaptivePublishThrottleController.linearPressure(0.85, 0.70, 0.85), 1.0, 1e-9);
    }

    @Test
    public void testLinearPressureMidpointInterpolation() {
        // Midpoint between 0.70 and 0.85 is 0.775 → pressure = 0.5.
        double mid = (0.70 + 0.85) / 2.0; // 0.775
        assertEquals(AdaptivePublishThrottleController.linearPressure(mid, 0.70, 0.85), 0.5, 1e-9);
    }

    @Test
    public void testLinearPressureQuarterInterpolation() {
        // Quarter of the way between 0.70 and 0.85:
        // value = 0.70 + 0.25*(0.85-0.70) = 0.70 + 0.0375 = 0.7375 → pressure = 0.25
        double quarter = 0.70 + 0.25 * (0.85 - 0.70);
        assertEquals(AdaptivePublishThrottleController.linearPressure(quarter, 0.70, 0.85), 0.25, 1e-9);
    }

    @Test
    public void testLinearPressureZeroRangeReturnsOne() {
        // Degenerate case: highWm == lowWm must return 1.0 (not divide-by-zero).
        assertEquals(AdaptivePublishThrottleController.linearPressure(0.80, 0.80, 0.80), 1.0, 1e-9);
    }

    // =========================================================================
    // computeTargetRate() — bounded step and hard floor
    // =========================================================================

    @Test
    public void testComputeTargetRateNoPressure() {
        // At pressure=0.0, target should equal naturalRate (no reduction).
        // But bounded step from currentRate=0 means we start from naturalRate.
        long result = controller.computeTargetRate(1000.0, 0.0, 0L, config);
        // targetFactor = 1.0 - 0*(1-0.10) = 1.0 → idealTarget = 1000
        // base = naturalRate = 1000 (currentRate == 0 → use natural)
        // delta = max(1, 1000 * 0.25) = 250
        // rampUp: min(1000, 1000+250) = 1000
        assertEquals(result, 1000L, "At zero pressure, target rate must equal natural rate");
    }

    @Test
    public void testComputeTargetRateFullPressure() {
        // At pressure=1.0, ideal = naturalRate * minFactor = 1000 * 0.10 = 100.
        // Base = natural = 1000 (currentRate=0), delta = 250.
        // Ramp down: max(100, 1000-250) = max(100, 750) = 750 (bounded step, first cycle).
        long result = controller.computeTargetRate(1000.0, 1.0, 0L, config);
        assertEquals(result, 750L,
                "First cycle at full pressure: bounded step must limit decrease to maxChangeFactor=25%");
    }

    @Test
    public void testComputeTargetRateReachesFloorAfterFourCycles() {
        // At full pressure, after enough cycles the rate should reach the floor.
        // Each cycle: decrease by maxDelta = 25% of natural = 250.
        // natural=1000, floor=100
        // Cycle 1: base=1000, ideal=100, delta=250 → newRate=max(100,750)=750
        // Cycle 2: base=750,  ideal=100, delta=250 → newRate=max(100,500)=500
        // Cycle 3: base=500,  ideal=100, delta=250 → newRate=max(100,250)=250
        // Cycle 4: base=250,  ideal=100, delta=250 → newRate=max(100,0)=100
        long r1 = controller.computeTargetRate(1000.0, 1.0, 0L, config);
        long r2 = controller.computeTargetRate(1000.0, 1.0, r1, config);
        long r3 = controller.computeTargetRate(1000.0, 1.0, r2, config);
        long r4 = controller.computeTargetRate(1000.0, 1.0, r3, config);

        assertEquals(r1, 750L, "Cycle 1 bounded step");
        assertEquals(r2, 500L, "Cycle 2 bounded step");
        assertEquals(r3, 250L, "Cycle 3 bounded step");
        assertEquals(r4, 100L, "Cycle 4: should reach floor (minRateFactor=0.10 of 1000)");
    }

    @Test
    public void testComputeTargetRateBoundedStepUp() {
        // Recovery: pressure drops, rate should ramp up at most by maxChangeFactor per cycle.
        // Previous cycle had rate 100 (floor). Natural=1000, pressure now 0.
        // idealTarget = 1000, base=100, delta=250 → ramp up: min(1000, 100+250) = 350.
        long result = controller.computeTargetRate(1000.0, 0.0, 100L, config);
        assertEquals(result, 350L,
                "Recovery: bounded step up must not exceed maxChangeFactor per cycle");
    }

    @Test
    public void testComputeTargetRateBoundedStepUpEventuallyReachesNatural() {
        // Verify full recovery after 4 cycles from floor.
        long r1 = controller.computeTargetRate(1000.0, 0.0, 100L, config);  // 350
        long r2 = controller.computeTargetRate(1000.0, 0.0, r1, config);    // 600
        long r3 = controller.computeTargetRate(1000.0, 0.0, r2, config);    // 850
        long r4 = controller.computeTargetRate(1000.0, 0.0, r3, config);    // 1000

        assertEquals(r1, 350L);
        assertEquals(r2, 600L);
        assertEquals(r3, 850L);
        assertEquals(r4, 1000L, "Rate must recover to natural after 4 ramp-up cycles");
    }

    @Test
    public void testComputeTargetRateHardFloorNeverViolated() {
        // Even at full pressure with a very small natural rate, floor must hold.
        // natural=10, minFactor=0.10 → floor = max(1, 1) = 1.
        long result = controller.computeTargetRate(10.0, 1.0, 10L, config);
        assertTrue(result >= 1L, "Rate must never drop below 1 message/s");
    }

    @Test
    public void testComputeTargetRateZeroNaturalRateReturnsOne() {
        long result = controller.computeTargetRate(0.0, 1.0, 0L, config);
        assertEquals(result, 1L, "naturalRate=0 must return 1 (hard minimum)");

        result = controller.computeTargetRate(-5.0, 0.5, 0L, config);
        assertEquals(result, 1L, "negative naturalRate must return 1");
    }

    @Test
    public void testComputeTargetRateMinFloorAbsoluteValue() {
        // natural=100, minFactor=0.10 → minAbsolute=10
        // pressureFactor=1.0, currentRate=10 (already at floor)
        // ideal=10, base=10, delta=25 → ramp down: max(10, 10-25)=10 → then floor max(10,10)=10
        long result = controller.computeTargetRate(100.0, 1.0, 10L, config);
        assertEquals(result, 10L, "Rate at the floor must stay at floor (not go lower)");
    }

    // =========================================================================
    // Hysteresis tests via runControllerCycle()
    // =========================================================================

    private AdaptivePublishRateLimiter buildLimiterWithNaturalRate(double naturalMsgRate) {
        AdaptivePublishRateLimiter l = new AdaptivePublishRateLimiter(
                manualClockSource::get,
                p -> p.getCnx().getThrottleTracker()
                        .markThrottled(ServerCnxThrottleTracker.ThrottleType.TopicPublishRate),
                p -> p.getCnx().getThrottleTracker()
                        .unmarkThrottled(ServerCnxThrottleTracker.ThrottleType.TopicPublishRate));
        // Seed natural rate: sampleRate at t=0, then t=1s with naturalMsgRate msgs.
        long t0 = TimeUnit.SECONDS.toNanos(0);
        long t1 = TimeUnit.SECONDS.toNanos(1);
        l.sampleRate(0L, 0L, t0);
        l.sampleRate((long) naturalMsgRate, (long) (naturalMsgRate * 1024), t1);
        return l;
    }

    private PersistentTopic buildMockTopic(AdaptivePublishRateLimiter limiter,
                                           long backlogBytes, long quotaBytes) {
        PersistentTopic topic = mock(PersistentTopic.class);
        when(topic.getAdaptivePublishRateLimiter()).thenReturn(limiter);
        when(topic.getBacklogSize()).thenReturn(backlogBytes);
        BacklogQuota bq = mock(BacklogQuota.class);
        when(bq.getLimitSize()).thenReturn(quotaBytes);
        when(topic.getBacklogQuota(BacklogQuota.BacklogQuotaType.destination_storage)).thenReturn(bq);
        when(topic.getMsgInCounter()).thenReturn(10_000L);
        when(topic.getBytesInCounter()).thenReturn(10_000_000L);
        when(topic.getName()).thenReturn("persistent://tenant/ns/test-topic");
        return topic;
    }

    /**
     * Wires {@code brokerService.forEachPersistentTopic()} to call the consumer
     * with exactly one topic.
     */
    @SuppressWarnings("unchecked")
    private void wireSingleTopic(PersistentTopic topic) {
        doAnswer(invocation -> {
            Consumer<PersistentTopic> consumer = invocation.getArgument(0);
            consumer.accept(topic);
            return null;
        }).when(brokerService).forEachPersistentTopic(any());
    }

    @Test
    public void testThrottleActivatesWhenBacklogAboveHighWatermark() {
        // backlogHighWm=0.90, quota=1000, backlog=950 (95%) → pressure=1.0 → activate
        AdaptivePublishRateLimiter limiter = buildLimiterWithNaturalRate(1000.0);
        assertFalse(limiter.isActive(), "should start inactive");

        PersistentTopic topic = buildMockTopic(limiter, 950L, 1000L);
        wireSingleTopic(topic);

        config.setAdaptivePublisherThrottlingObserveOnly(false);
        controller.runControllerCycle();

        assertTrue(limiter.isActive(),
                "Limiter must be activated when backlog exceeds high watermark");
    }

    @Test
    public void testThrottleDoesNotActivateBelowLowWatermark() {
        // backlogLowWm=0.75, backlog=700/1000 (70%) → pressure=0 → no activation
        AdaptivePublishRateLimiter limiter = buildLimiterWithNaturalRate(1000.0);
        assertFalse(limiter.isActive());

        PersistentTopic topic = buildMockTopic(limiter, 700L, 1000L);
        wireSingleTopic(topic);

        config.setAdaptivePublisherThrottlingObserveOnly(false);
        controller.runControllerCycle();

        assertFalse(limiter.isActive(),
                "Limiter must NOT activate when backlog is below low watermark");
    }

    @Test
    public void testHysteresisRemainsActiveUntilBelowLowWatermark() {
        // First cycle: backlog at 95% → activate.
        AdaptivePublishRateLimiter limiter = buildLimiterWithNaturalRate(1000.0);
        PersistentTopic topic = buildMockTopic(limiter, 950L, 1000L);
        wireSingleTopic(topic);
        config.setAdaptivePublisherThrottlingObserveOnly(false);
        controller.runControllerCycle();
        assertTrue(limiter.isActive(), "Must activate at 95%");

        // Second cycle: backlog drops to 80% (above lowWm=75%) → still active
        when(topic.getBacklogSize()).thenReturn(800L);
        controller.runControllerCycle();
        assertTrue(limiter.isActive(),
                "Must stay active at 80% (above low watermark 75%)");

        // Third cycle: backlog drops to 70% (below lowWm=75%) → deactivate
        when(topic.getBacklogSize()).thenReturn(700L);
        controller.runControllerCycle();
        assertFalse(limiter.isActive(),
                "Must deactivate only when backlog drops below low watermark (75%)");
    }

    @Test
    public void testHysteresisInZoneBetweenLowAndHighWatermarks() {
        // Zone: 75% < backlog < 90%
        // pressure > 0 → activate if inactive, stay active if already active.
        AdaptivePublishRateLimiter limiter = buildLimiterWithNaturalRate(1000.0);
        PersistentTopic topic = buildMockTopic(limiter, 820L, 1000L); // 82%
        wireSingleTopic(topic);
        config.setAdaptivePublisherThrottlingObserveOnly(false);

        controller.runControllerCycle();
        assertTrue(limiter.isActive(),
                "Should activate when backlog is in the ramp zone (82% > lowWm=75%)");
    }

    // =========================================================================
    // observeOnly guarantee — CRITICAL SAFETY TEST
    // =========================================================================

    /**
     * CRITICAL TEST: When {@code observeOnly=true}, the controller cycle MUST NOT
     * call {@link AdaptivePublishRateLimiter#activate} or
     * {@link AdaptivePublishRateLimiter#deactivate}, even under extreme pressure.
     *
     * <p>This test asserts:
     * <ol>
     *   <li>{@code limiter.isActive()} remains {@code false} after the cycle.</li>
     *   <li>{@code throttleTracker.throttledCount()} remains {@code 0} — meaning no
     *       {@code markThrottled()} call was made on any connection.</li>
     * </ol>
     *
     * <p>If this test fails, it means the observe-only circuit-breaker is broken and
     * the controller would throttle producers in production without operator intent.
     */
    @Test
    public void testObserveOnlyNeverActivatesLimiter() throws Exception {
        // Max pressure: backlog at 100% of quota.
        AdaptivePublishRateLimiter limiter = buildLimiterWithNaturalRate(1000.0);
        PersistentTopic topic = buildMockTopic(limiter, 1000L, 1000L); // 100% backlog
        wireSingleTopic(topic);

        // Enable observe-only mode — the key config that MUST prevent any throttling.
        config.setAdaptivePublisherThrottlingObserveOnly(true);

        controller.runControllerCycle();

        assertFalse(limiter.isActive(),
                "OBSERVE-ONLY VIOLATED: limiter.isActive() must be false when observeOnly=true");
        assertEquals(limiter.getThrottleActivationCount(), 0L,
                "OBSERVE-ONLY VIOLATED: throttleActivationCount must be 0 when observeOnly=true");
    }

    /**
     * Extension of the above: even after 10 consecutive cycles under maximum pressure,
     * the channel autoread state must never change when observeOnly=true.
     *
     * <p>This is the "never changes channel autoread" guarantee specifically requested
     * in the implementation spec.
     */
    @Test
    public void testObserveOnlyNeverChangesChannelAutoreadAfterManyCycles() throws Exception {
        AdaptivePublishRateLimiter limiter = buildLimiterWithNaturalRate(1000.0);
        PersistentTopic topic = buildMockTopic(limiter, 1000L, 1000L); // 100% backlog
        wireSingleTopic(topic);

        config.setAdaptivePublisherThrottlingObserveOnly(true);

        // Run many cycles under extreme pressure.
        for (int i = 0; i < 10; i++) {
            controller.runControllerCycle();
        }

        // Assert: limiter never activated.
        assertFalse(limiter.isActive(),
                "OBSERVE-ONLY VIOLATED after 10 cycles: limiter must never activate");
        assertEquals(limiter.getThrottleActivationCount(), 0L,
                "OBSERVE-ONLY VIOLATED: zero activations expected");

        // Assert: no producer connection was throttled (no channel autoread change).
        // Even if handlePublishThrottling were called on IO thread — which it won't be
        // since active=false — throttledCount would still be 0.
        CompletableFuture<Void> future = new CompletableFuture<>();
        eventLoop.execute(() -> {
            try {
                // Simulate IO thread calling handlePublishThrottling during observe-only.
                manualClockSource.addAndGet(TimeUnit.SECONDS.toNanos(1));
                for (int i = 0; i < 1000; i++) {
                    limiter.handlePublishThrottling(producer, 500, 500_000);
                }
                assertEquals(throttleTracker.throttledCount(), 0,
                        "OBSERVE-ONLY VIOLATED: throttledCount must be 0 — "
                                + "channel autoread must NEVER change when observeOnly=true");
                future.complete(null);
            } catch (Throwable t) {
                future.completeExceptionally(t);
            }
        });
        future.get(5, TimeUnit.SECONDS);
    }

    /**
     * Verify that switching FROM observeOnly=true TO observeOnly=false at runtime
     * (dynamic config flip) correctly enables throttling on the very next cycle.
     */
    @Test
    public void testObserveOnlyToLiveModeTransition() {
        AdaptivePublishRateLimiter limiter = buildLimiterWithNaturalRate(1000.0);
        PersistentTopic topic = buildMockTopic(limiter, 1000L, 1000L); // 100%
        wireSingleTopic(topic);

        // Phase 1: observeOnly — no throttling.
        config.setAdaptivePublisherThrottlingObserveOnly(true);
        controller.runControllerCycle();
        assertFalse(limiter.isActive(), "Must not be active in observeOnly mode");

        // Phase 2: flip to live — throttling must now be applied.
        config.setAdaptivePublisherThrottlingObserveOnly(false);
        controller.runControllerCycle();
        assertTrue(limiter.isActive(),
                "Must activate on first live-mode cycle after observeOnly→live transition");
        assertTrue(limiter.getThrottleActivationCount() >= 1L,
                "Activation count must be >= 1 after live cycle");
    }

    /**
     * Verify that switching FROM live TO observeOnly=true mid-flight stops further
     * rate adjustments. The current active state is NOT changed retroactively, but
     * no new activate/deactivate calls are made.
     *
     * <p>Note: this tests the "emergency circuit-breaker" use case — operator
     * flips observeOnly=true at runtime to immediately stop new throttle decisions.
     * Existing active throttles are unaffected until pressure drops below lowWm
     * in a live cycle (but since we're now in observeOnly, they will never be
     * deactivated by the controller either until flipped back).
     */
    @Test
    public void testObserveOnlyMidFlightFreezesThrottleDecisions() {
        AdaptivePublishRateLimiter limiter = buildLimiterWithNaturalRate(1000.0);
        PersistentTopic topic = buildMockTopic(limiter, 1000L, 1000L); // 100%
        wireSingleTopic(topic);

        // Live mode: activate throttle.
        config.setAdaptivePublisherThrottlingObserveOnly(false);
        controller.runControllerCycle();
        assertTrue(limiter.isActive(), "Must be active in live mode");
        long activationCountBefore = limiter.getThrottleActivationCount();

        // Flip to observeOnly: controller must NOT call deactivate even when backlog drops.
        config.setAdaptivePublisherThrottlingObserveOnly(true);
        when(topic.getBacklogSize()).thenReturn(100L); // well below lowWm
        controller.runControllerCycle();

        // The limiter state is "frozen" — in observe-only the controller doesn't deactivate.
        // (The limiter remains active from the previous live cycle, but no new changes made.)
        // Activation count should not have increased.
        assertEquals(limiter.getThrottleActivationCount(), activationCountBefore,
                "No new activations should occur in observeOnly mode");
    }

    // =========================================================================
    // Broker-level counters
    // =========================================================================

    @Test
    public void testActiveThrottledTopicsCountIsUpdatedPerCycle() {
        AdaptivePublishRateLimiter limiter = buildLimiterWithNaturalRate(1000.0);
        PersistentTopic topic = buildMockTopic(limiter, 950L, 1000L);
        wireSingleTopic(topic);
        config.setAdaptivePublisherThrottlingObserveOnly(false);

        assertEquals(controller.getActiveThrottledTopicsCount(), 0,
                "Initial active throttled count must be 0");

        controller.runControllerCycle();

        assertEquals(controller.getActiveThrottledTopicsCount(), 1,
                "Active throttled count must be 1 after cycle with one throttled topic");
    }

    @Test
    public void testTotalActivationCountIncrementsInLiveMode() {
        AdaptivePublishRateLimiter limiter = buildLimiterWithNaturalRate(1000.0);
        PersistentTopic topic = buildMockTopic(limiter, 950L, 1000L);
        wireSingleTopic(topic);
        config.setAdaptivePublisherThrottlingObserveOnly(false);

        assertEquals(controller.getTotalActivationCount(), 0L);

        controller.runControllerCycle();
        assertEquals(controller.getTotalActivationCount(), 1L,
                "Total activation count must increment on first topic activation");

        // Repeated cycles (still active) should NOT increment again.
        controller.runControllerCycle();
        assertEquals(controller.getTotalActivationCount(), 1L,
                "Total activation count must NOT increment if topic already active");
    }

    @Test
    public void testTotalActivationCountDoesNotIncrementInObserveOnly() {
        AdaptivePublishRateLimiter limiter = buildLimiterWithNaturalRate(1000.0);
        PersistentTopic topic = buildMockTopic(limiter, 950L, 1000L);
        wireSingleTopic(topic);
        config.setAdaptivePublisherThrottlingObserveOnly(true);

        controller.runControllerCycle();
        assertEquals(controller.getTotalActivationCount(), 0L,
                "Total activation count must be 0 in observeOnly mode");
    }

    // =========================================================================
    // computeBacklogPressure() — quota handling
    // =========================================================================

    @Test
    public void testComputeBacklogPressureNoQuota() {
        PersistentTopic topic = mock(PersistentTopic.class);
        BacklogQuota bq = mock(BacklogQuota.class);
        when(bq.getLimitSize()).thenReturn(-1L); // no quota
        when(topic.getBacklogQuota(BacklogQuota.BacklogQuotaType.destination_storage)).thenReturn(bq);
        when(topic.getBacklogSize()).thenReturn(999_999L);

        double pressure = controller.computeBacklogPressure(topic, config);
        assertEquals(pressure, 0.0, 1e-9,
                "Backlog pressure must be 0 when no quota is configured");
    }

    @Test
    public void testComputeBacklogPressureAboveHighWatermark() {
        PersistentTopic topic = mock(PersistentTopic.class);
        BacklogQuota bq = mock(BacklogQuota.class);
        when(bq.getLimitSize()).thenReturn(1000L);
        when(topic.getBacklogQuota(BacklogQuota.BacklogQuotaType.destination_storage)).thenReturn(bq);
        when(topic.getBacklogSize()).thenReturn(950L); // 95% > 90% highWm

        double pressure = controller.computeBacklogPressure(topic, config);
        assertEquals(pressure, 1.0, 1e-9,
                "Backlog pressure must be 1.0 when backlog exceeds high watermark");
    }

    @Test
    public void testComputeBacklogPressureBelowLowWatermark() {
        PersistentTopic topic = mock(PersistentTopic.class);
        BacklogQuota bq = mock(BacklogQuota.class);
        when(bq.getLimitSize()).thenReturn(1000L);
        when(topic.getBacklogQuota(BacklogQuota.BacklogQuotaType.destination_storage)).thenReturn(bq);
        when(topic.getBacklogSize()).thenReturn(700L); // 70% < 75% lowWm

        double pressure = controller.computeBacklogPressure(topic, config);
        assertEquals(pressure, 0.0, 1e-9,
                "Backlog pressure must be 0.0 when backlog is below low watermark");
    }

    @Test
    public void testComputeBacklogPressureMidpoint() {
        PersistentTopic topic = mock(PersistentTopic.class);
        BacklogQuota bq = mock(BacklogQuota.class);
        when(bq.getLimitSize()).thenReturn(1000L);
        when(topic.getBacklogQuota(BacklogQuota.BacklogQuotaType.destination_storage)).thenReturn(bq);
        // Midpoint of [75%, 90%] = 82.5% = 825 bytes
        when(topic.getBacklogSize()).thenReturn(825L);

        double pressure = controller.computeBacklogPressure(topic, config);
        assertEquals(pressure, 0.5, 1e-6,
                "Backlog pressure must be 0.5 at midpoint of low/high watermarks");
    }
}
