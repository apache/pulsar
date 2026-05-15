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
package org.apache.pulsar.client.impl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import io.netty.channel.EventLoopGroup;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.pulsar.client.impl.SameAuthParamsLookupAutoClusterFailover.PulsarServiceState;
import org.apache.pulsar.client.util.ExecutorProvider;
import org.apache.pulsar.common.util.netty.EventLoopUtil;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker-impl")
public class SameAuthParamsLookupAutoClusterFailoverTest {

    private static final String URL0 = "pulsar://broker0:6650";
    private static final String URL1 = "pulsar://broker1:6650";
    private static final String URL2 = "pulsar://broker2:6650";

    private EventLoopGroup executor;
    private PulsarClientImpl mockClient;
    private SameAuthParamsLookupAutoClusterFailover failover;
    private PulsarServiceState[] stateArray;
    private MutableInt[] counterArray;

    @BeforeMethod
    public void setup() throws Exception {
        executor = EventLoopUtil.newEventLoopGroup(1, false,
                new ExecutorProvider.ExtendedThreadFactory("test-failover"));

        String[] urlArray = new String[]{URL0, URL1, URL2};
        failover = SameAuthParamsLookupAutoClusterFailover.builder()
                .pulsarServiceUrlArray(urlArray)
                .failoverThreshold(1)
                .recoverThreshold(2)
                .checkHealthyIntervalMs(100)
                .testTopic("a/b/c")
                .markTopicNotFoundAsAvailable(true)
                .build();

        mockClient = mock(PulsarClientImpl.class);
        doNothing().when(mockClient).updateServiceUrl(anyString());
        doNothing().when(mockClient).reloadLookUp();

        FieldUtils.writeField(failover, "pulsarClient", mockClient, true);
        FieldUtils.writeField(failover, "executor", executor, true);

        stateArray = (PulsarServiceState[]) FieldUtils.readField(failover, "pulsarServiceStateArray", true);
        counterArray = (MutableInt[]) FieldUtils.readField(failover, "checkCounterArray", true);
    }

    @AfterMethod(alwaysRun = true)
    public void cleanup() {
        if (executor != null) {
            executor.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS);
        }
    }

    private void setLookupResult(String url, boolean available) {
        LookupService lookup = mock(LookupService.class);
        if (available) {
            InetSocketAddress addr = InetSocketAddress.createUnresolved("broker", 6650);
            when(lookup.getBroker(any()))
                    .thenReturn(CompletableFuture.completedFuture(
                            new LookupTopicResult(addr, addr, false)));
        } else {
            when(lookup.getBroker(any()))
                    .thenReturn(CompletableFuture.failedFuture(
                            new RuntimeException("connection refused")));
        }
        when(mockClient.getLookup(url)).thenReturn(lookup);
    }

    /**
     * Reproduces the race condition where findFailoverTo() skips over an unavailable service
     * without marking it as Failed. This leaves a stale Healthy state that causes a spurious
     * recovery bounce (0 -> 2 -> 1 -> 2) instead of a clean failover (0 -> 2).
     *
     * <p>Before the fix, after failover from index 0 to 2, state[1] remained Healthy (stale).
     * On the next check cycle, firstHealthyPulsarService() would see state[1]=Healthy and
     * immediately "recover" to index 1 — which is actually a broken service. This caused
     * unnecessary bouncing and, combined with 3-second probe timeouts on dead services,
     * could push the total failover time past the test's awaitility timeout.
     */
    @Test(timeOut = 30000)
    public void testFindFailoverToMarksSkippedServicesAsFailed() throws Exception {
        // url0 is down, url1 is down, url2 is healthy.
        setLookupResult(URL0, false);
        setLookupResult(URL1, false);
        setLookupResult(URL2, true);

        // Pre-set state[0] to Failed (as if checkPulsarServices already detected it),
        // then run one check cycle. All on the executor to ensure thread safety.
        runOnExecutor(() -> {
            stateArray[0] = PulsarServiceState.Failed;
            counterArray[0].setValue(0);
        });
        runCheckCycle();

        // After the fix, findFailoverTo marks url1 as Failed when it fails probing.
        // Verify on the executor thread where state is owned.
        runOnExecutor(() -> {
            assertEquals(failover.getCurrentPulsarServiceIndex(), 2,
                    "Should have failed over to index 2");
            assertEquals(stateArray[1], PulsarServiceState.Failed,
                    "Service 1 should be marked Failed by findFailoverTo, not remain stale Healthy");
            assertEquals(stateArray[2], PulsarServiceState.Healthy,
                    "Service 2 should remain Healthy");
        });
    }

    /**
     * Verifies no spurious recovery bounce occurs after failover. Without the fix,
     * the first check cycle after failover to index 2 would see stale Healthy state[1]
     * and immediately switch to index 1.
     */
    @Test(timeOut = 30000)
    public void testNoSpuriousRecoveryBounceAfterFailover() throws Exception {
        // url0 is down, url1 is down, url2 is healthy.
        setLookupResult(URL0, false);
        setLookupResult(URL1, false);
        setLookupResult(URL2, true);

        // Pre-set state[0] to Failed.
        runOnExecutor(() -> {
            stateArray[0] = PulsarServiceState.Failed;
            counterArray[0].setValue(0);
        });

        // Failover: 0 -> 2.
        runCheckCycle();
        runOnExecutor(() -> assertEquals(failover.getCurrentPulsarServiceIndex(), 2));

        // Run another check cycle. Without the fix, state[1] would be stale Healthy,
        // and firstHealthyPulsarService would return 1, causing a spurious switch.
        runCheckCycle();
        runOnExecutor(() -> assertEquals(failover.getCurrentPulsarServiceIndex(), 2,
                "Should stay at index 2, not bounce to index 1"));
    }

    /**
     * Verifies that recovery still works correctly for a service that was marked Failed
     * by findFailoverTo, once that service becomes available again.
     */
    @Test(timeOut = 30000)
    public void testRecoveryAfterFindFailoverToMarksServiceFailed() throws Exception {
        // url0 is down, url1 is down, url2 is healthy.
        setLookupResult(URL0, false);
        setLookupResult(URL1, false);
        setLookupResult(URL2, true);

        // Pre-set state[0] to Failed and trigger failover 0 -> 2.
        runOnExecutor(() -> {
            stateArray[0] = PulsarServiceState.Failed;
            counterArray[0].setValue(0);
        });
        runCheckCycle();
        runOnExecutor(() -> {
            assertEquals(failover.getCurrentPulsarServiceIndex(), 2);
            assertEquals(stateArray[1], PulsarServiceState.Failed);
        });

        // Now make url1 healthy (simulating recovery of that service).
        setLookupResult(URL1, true);

        // Run check cycles until service 1 recovers.
        // Failed -> PreRecover (1 check) -> Healthy (recoverThreshold=2, so 1 more check).
        Awaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            runCheckCycle();
            runOnExecutor(() -> {
                assertEquals(failover.getCurrentPulsarServiceIndex(), 1,
                        "Should recover to index 1 after it becomes healthy");
                assertEquals(stateArray[1], PulsarServiceState.Healthy);
            });
        });
    }

    private void runOnExecutor(Runnable task) throws Exception {
        try {
            executor.submit(task).get(5, TimeUnit.SECONDS);
        } catch (java.util.concurrent.ExecutionException e) {
            // Unwrap so that AssertionErrors propagate directly to Awaitility.
            if (e.getCause() instanceof AssertionError) {
                throw (AssertionError) e.getCause();
            }
            throw e;
        }
    }

    private void runCheckCycle() throws Exception {
        runOnExecutor(() -> {
            try {
                Method checkMethod = SameAuthParamsLookupAutoClusterFailover.class
                        .getDeclaredMethod("checkPulsarServices");
                checkMethod.setAccessible(true);
                Method firstHealthyMethod = SameAuthParamsLookupAutoClusterFailover.class
                        .getDeclaredMethod("firstHealthyPulsarService");
                firstHealthyMethod.setAccessible(true);
                Method findFailoverMethod = SameAuthParamsLookupAutoClusterFailover.class
                        .getDeclaredMethod("findFailoverTo");
                findFailoverMethod.setAccessible(true);
                Method updateMethod = SameAuthParamsLookupAutoClusterFailover.class
                        .getDeclaredMethod("updateServiceUrl", int.class);
                updateMethod.setAccessible(true);

                checkMethod.invoke(failover);
                int firstHealthy = (int) firstHealthyMethod.invoke(failover);
                int currentIndex = failover.getCurrentPulsarServiceIndex();
                if (firstHealthy != currentIndex) {
                    if (firstHealthy < 0) {
                        int failoverTo = (int) findFailoverMethod.invoke(failover);
                        if (failoverTo >= 0) {
                            updateMethod.invoke(failover, failoverTo);
                        }
                    } else {
                        updateMethod.invoke(failover, firstHealthy);
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }
}
