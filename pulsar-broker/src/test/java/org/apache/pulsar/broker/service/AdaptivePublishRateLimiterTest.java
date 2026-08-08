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
import org.apache.pulsar.broker.qos.MonotonicClock;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Unit tests for {@link AdaptivePublishRateLimiter}.
 *
 * <p>Tests are divided into:
 * <ul>
 *   <li>No-op guarantee: {@code handlePublishThrottling()} must be a complete no-op when inactive.</li>
 *   <li>Activation/deactivation cycle: correct state transitions and counter increments.</li>
 *   <li>Rate sampling and asymmetric EWMA correctness.</li>
 *   <li>observeOnly safety: channel autoread is NEVER changed when observeOnly is in effect.</li>
 * </ul>
 */
@Test(groups = "broker")
public class AdaptivePublishRateLimiterTest {

    private static final long ONE_SECOND_NS = TimeUnit.SECONDS.toNanos(1);

    private AtomicLong manualClockSource;
    private MonotonicClock clock;

    private Producer producer;
    private ServerCnx serverCnx;
    private ServerCnxThrottleTracker throttleTracker;
    private DefaultEventLoop eventLoop;

    /** The limiter under test. */
    private AdaptivePublishRateLimiter limiter;

    @BeforeMethod
    public void setup() throws Exception {
        manualClockSource = new AtomicLong(TimeUnit.SECONDS.toNanos(100));
        clock = manualClockSource::get;

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

        BrokerService brokerService = mock(BrokerService.class);
        when(serverCnx.getBrokerService()).thenReturn(brokerService);
        EventLoopGroup eventLoopGroup = mock(EventLoopGroup.class);
        when(brokerService.executor()).thenReturn(eventLoopGroup);
        when(eventLoopGroup.next()).thenReturn(eventLoop);

        limiter = new AdaptivePublishRateLimiter(
                clock,
                p -> p.getCnx().getThrottleTracker()
                        .markThrottled(ServerCnxThrottleTracker.ThrottleType.AdaptivePublishRate),
                p -> p.getCnx().getThrottleTracker()
                        .unmarkThrottled(ServerCnxThrottleTracker.ThrottleType.AdaptivePublishRate));
    }

    @AfterMethod
    public void tearDown() throws Exception {
        eventLoop.shutdownGracefully().sync();
    }

    // -------------------------------------------------------------------------
    // No-op guarantee when inactive
    // -------------------------------------------------------------------------

    /**
     * When the limiter is inactive (never activated), repeated calls to
     * {@code handlePublishThrottling()} must NOT change the connection's
     * {@code autoRead} state in any way — {@code throttledCount()} must
     * remain exactly 0.
     */
    @Test
    public void testInactiveIsCompleteNoop() throws Exception {
        assertFalse(limiter.isActive(), "limiter must start inactive");

        CompletableFuture<Void> future = new CompletableFuture<>();
        eventLoop.execute(() -> {
            try {
                // Even a large publish batch must not throttle when inactive.
                for (int i = 0; i < 1000; i++) {
                    limiter.handlePublishThrottling(producer, 100, 1_000_000);
                }
                assertEquals(throttleTracker.throttledCount(), 0,
                        "throttledCount must be 0 when limiter is inactive");
                future.complete(null);
            } catch (Throwable t) {
                future.completeExceptionally(t);
            }
        });
        future.get(5, TimeUnit.SECONDS);
    }

    /**
     * Verifies that the limiter's {@code update()} methods (called by the policy
     * framework) are intentional no-ops and do NOT activate throttling.
     */
    @Test
    public void testUpdateMethodsAreNoOps() throws Exception {
        assertFalse(limiter.isActive());

        limiter.update(new org.apache.pulsar.common.policies.data.Policies(), "cluster");
        limiter.update(new org.apache.pulsar.common.policies.data.PublishRate(100, 1000L));

        assertFalse(limiter.isActive(),
                "update() must never activate the adaptive limiter");
    }

    // -------------------------------------------------------------------------
    // activate() / deactivate() lifecycle
    // -------------------------------------------------------------------------

    @Test
    public void testActivateEnablesThrottling() throws Exception {
        // Activate at very low rate (1 msg/s) so first publish triggers throttle.
        limiter.activate(1L, 1L);
        assertTrue(limiter.isActive());
        assertEquals(limiter.getCurrentEffectiveMsgRate(), 1L);

        CompletableFuture<Void> future = new CompletableFuture<>();
        eventLoop.execute(() -> {
            try {
                // Advance clock so token bucket is exhausted immediately.
                manualClockSource.addAndGet(ONE_SECOND_NS);
                // Publish far more than 1 msg/s.
                limiter.handlePublishThrottling(producer, 100, 100_000);
                // The delegate token bucket should have been exhausted and markThrottled called.
                assertTrue(throttleTracker.throttledCount() > 0,
                        "throttledCount must be > 0 after activating with rate=1 and publishing 100 msgs");
                future.complete(null);
            } catch (Throwable t) {
                future.completeExceptionally(t);
            }
        });
        future.get(5, TimeUnit.SECONDS);
    }

    @Test
    public void testDeactivateRestoresNoop() throws Exception {
        limiter.activate(1L, 1L);
        assertTrue(limiter.isActive());

        limiter.deactivate();
        assertFalse(limiter.isActive());
        assertEquals(limiter.getCurrentEffectiveMsgRate(), 0L);
        assertEquals(limiter.getCurrentEffectiveByteRate(), 0L);

        CompletableFuture<Void> future = new CompletableFuture<>();
        eventLoop.execute(() -> {
            try {
                manualClockSource.addAndGet(ONE_SECOND_NS);
                for (int i = 0; i < 500; i++) {
                    limiter.handlePublishThrottling(producer, 100, 100_000);
                }
                assertEquals(throttleTracker.throttledCount(), 0,
                        "throttledCount must be 0 after deactivate()");
                future.complete(null);
            } catch (Throwable t) {
                future.completeExceptionally(t);
            }
        });
        future.get(5, TimeUnit.SECONDS);
    }

    @Test
    public void testActivationCountIncrementOnFirstActivate() {
        assertEquals(limiter.getThrottleActivationCount(), 0L);
        limiter.activate(100L, 10000L);
        assertEquals(limiter.getThrottleActivationCount(), 1L,
                "activation count must be 1 after first activate()");
    }

    @Test
    public void testActivationCountNotDoubledByRepeatedActivate() {
        limiter.activate(100L, 10000L);
        limiter.activate(80L, 8000L);
        limiter.activate(60L, 6000L);
        assertEquals(limiter.getThrottleActivationCount(), 1L,
                "activation count must be 1 regardless of repeated activate() without deactivate()");
    }

    @Test
    public void testActivationCountIncrementsPerActivateDeactivateCycle() {
        limiter.activate(100L, 10000L);
        limiter.deactivate();
        limiter.activate(100L, 10000L);
        limiter.deactivate();
        limiter.activate(100L, 10000L);
        assertEquals(limiter.getThrottleActivationCount(), 3L,
                "activation count must increment once per activate/deactivate cycle");
    }

    // -------------------------------------------------------------------------
    // Rate sampling and asymmetric EWMA
    // -------------------------------------------------------------------------

    @Test
    public void testSampleRateSkipsFirstCycle() {
        // When lastSampleTimeNs == 0, the first call sets the baseline only.
        long t0 = manualClockSource.get();
        limiter.sampleRate(1000L, 100_000L, t0);
        // Natural rate estimate must still be 0.0 — no elapsed time computed yet.
        assertEquals(limiter.getNaturalMsgRateEstimate(), 0.0, 1e-9,
                "First sampleRate() call must not compute EWMA (no elapsed interval)");
    }

    @Test
    public void testSampleRateDeltaComputation() {
        // Seed baseline at t=0.
        long t0 = TimeUnit.SECONDS.toNanos(0);
        limiter.sampleRate(0L, 0L, t0);

        // At t=1s, 100 messages have been published.
        long t1 = TimeUnit.SECONDS.toNanos(1);
        limiter.sampleRate(100L, 50_000L, t1);

        // Instant rate = 100 msg/s. EWMA from 0: 0*0.70 + 100*0.30 = 30 msg/s (up path, α=0.30).
        assertEquals(limiter.getNaturalMsgRateEstimate(), 30.0, 1e-6,
                "EWMA from 0 with instant=100 and alpha_up=0.30 should be 30.0");
    }

    @Test
    public void testAsymmetricEwmaRisesQuicklyOnIncrease() {
        long t0 = TimeUnit.SECONDS.toNanos(0);
        // Seed with 100 msg/s estimate by doing multiple samples.
        // First set the estimate to ~100 by seeding.
        limiter.sampleRate(0L, 0L, t0);
        // t=1s: 100 msgs → instant=100
        limiter.sampleRate(100L, 0L, TimeUnit.SECONDS.toNanos(1));
        // t=2s: 200 msgs → instant=100 (same rate; will not change much)
        limiter.sampleRate(200L, 0L, TimeUnit.SECONDS.toNanos(2));
        // t=3s: 300 msgs → instant=100
        limiter.sampleRate(300L, 0L, TimeUnit.SECONDS.toNanos(3));

        double baseline = limiter.getNaturalMsgRateEstimate();

        // Now spike to 200 msg/s — should rise quickly (α=0.30).
        limiter.sampleRate(500L, 0L, TimeUnit.SECONDS.toNanos(4)); // 200 msg in 1s
        double afterSpike = limiter.getNaturalMsgRateEstimate();

        assertTrue(afterSpike > baseline,
                "Estimate must rise when instant rate exceeds current estimate");
        // With α=0.30: afterSpike = baseline*0.70 + 200*0.30
        double expected = baseline * 0.70 + 200.0 * 0.30;
        assertEquals(afterSpike, expected, 1e-6, "Up-path EWMA must use alpha=0.30");
    }

    @Test
    public void testAsymmetricEwmaFallsSlowlyOnDecrease() {
        // Establish a high estimate first.
        limiter.sampleRate(0L, 0L, TimeUnit.SECONDS.toNanos(0));
        for (int i = 1; i <= 10; i++) {
            // 1000 msg/s for 10 seconds
            limiter.sampleRate((long) i * 1000L, 0L, TimeUnit.SECONDS.toNanos(i));
        }
        double highEstimate = limiter.getNaturalMsgRateEstimate();
        assertTrue(highEstimate > 500.0, "Estimate should be well above 500 after 10 cycles at 1000/s");

        // Now drop to 0 msg/s for one cycle.
        limiter.sampleRate(10_000L, 0L, TimeUnit.SECONDS.toNanos(11)); // same count = 0/s
        double afterDrop = limiter.getNaturalMsgRateEstimate();

        // With α_down=0.05: afterDrop = highEstimate*0.95 + 0*0.05 = highEstimate*0.95
        double expected = highEstimate * 0.95;
        assertEquals(afterDrop, expected, 1e-6, "Down-path EWMA must use alpha=0.05");
        // Should NOT have fallen by more than 5%.
        assertTrue(afterDrop > highEstimate * 0.90,
                "Estimate must NOT fall more than 5% in one cycle (slow decay)");
    }

    // -------------------------------------------------------------------------
    // observeOnly safety: channel autoread must NEVER change
    // -------------------------------------------------------------------------

    /**
     * CRITICAL SAFETY TEST: When the AdaptivePublishRateLimiter is used with
     * observe-only semantics — meaning the controller has NOT called
     * {@code activate()} — the limiter MUST be a complete no-op.
     *
     * <p>This test simulates the observe-only guarantee from the limiter's
     * perspective: since the controller never calls {@code activate()} when
     * {@code observeOnly=true}, {@code isActive()} stays {@code false}, and
     * {@code handlePublishThrottling()} must never change channel autoread state.
     *
     * <p>If this test fails, it means some code path is changing
     * {@code ServerCnxThrottleTracker} state without going through the
     * active flag, which would be a critical bug.
     */
    @Test
    public void testObserveOnlyLimiterNeverChangesChannelAutoread() throws Exception {
        // The limiter starts inactive — simulating observeOnly (controller never called activate()).
        assertFalse(limiter.isActive(),
                "Limiter must start inactive (simulating observeOnly=true)");

        CompletableFuture<Void> future = new CompletableFuture<>();
        eventLoop.execute(() -> {
            try {
                manualClockSource.addAndGet(ONE_SECOND_NS);
                // Simulate many large publish operations as if backlog/memory is under extreme pressure.
                for (int cycle = 0; cycle < 100; cycle++) {
                    manualClockSource.addAndGet(ONE_SECOND_NS);
                    limiter.handlePublishThrottling(producer, 10_000, 100_000_000);
                }

                // The critical assertion: zero throttle count.
                assertEquals(throttleTracker.throttledCount(), 0,
                        "observeOnly guarantee violated: throttledCount must be 0 "
                                + "when limiter has never been activated");
                future.complete(null);
            } catch (Throwable t) {
                future.completeExceptionally(t);
            }
        });
        future.get(5, TimeUnit.SECONDS);
    }

    /**
     * Extension of the above: even after {@code deactivate()} is called (which is
     * what the controller does in observe-only mode when it would normally deactivate),
     * repeated calls must leave the throttle count at 0.
     */
    @Test
    public void testDeactivateOnNeverActiveLimiterIsHarmless() throws Exception {
        assertFalse(limiter.isActive());
        limiter.deactivate(); // Should be a no-op — limiter was never active.
        assertFalse(limiter.isActive());

        CompletableFuture<Void> future = new CompletableFuture<>();
        eventLoop.execute(() -> {
            try {
                manualClockSource.addAndGet(ONE_SECOND_NS);
                limiter.handlePublishThrottling(producer, 50_000, 500_000_000);
                assertEquals(throttleTracker.throttledCount(), 0,
                        "throttledCount must be 0 after deactivate() on a never-activated limiter");
                future.complete(null);
            } catch (Throwable t) {
                future.completeExceptionally(t);
            }
        });
        future.get(5, TimeUnit.SECONDS);
    }
}
