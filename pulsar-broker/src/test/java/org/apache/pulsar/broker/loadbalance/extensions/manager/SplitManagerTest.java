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
package org.apache.pulsar.broker.loadbalance.extensions.manager;

import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateChannelImpl.VERSION_ID_INIT;
import static org.apache.pulsar.broker.loadbalance.extensions.models.SplitDecision.Reason.Sessions;
import static org.apache.pulsar.broker.loadbalance.extensions.models.SplitDecision.Reason.Unknown;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateData;
import org.apache.pulsar.broker.loadbalance.extensions.models.SplitCounter;
import org.apache.pulsar.broker.loadbalance.extensions.models.SplitDecision;
import org.apache.pulsar.common.util.FutureUtil;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker")
public class SplitManagerTest {
    
    String bundle = "bundle-1";

    String dstBroker = "broker-1";

    @Test
    public void testEventPubFutureHasException() {
        var counter = new SplitCounter();
        SplitManager manager = new SplitManager(counter);
        var decision = new SplitDecision();
        CompletableFuture<Void> future =
                manager.waitAsync(FutureUtil.failedFuture(new Exception("test")),
                        bundle, decision, 10, TimeUnit.SECONDS);

        assertTrue(future.isCompletedExceptionally());
        try {
            future.get();
            fail();
        } catch (Exception ex) {
            assertEquals(ex.getCause().getMessage(), "test");
        }
        var counterExpected = new SplitCounter();
        counterExpected.update(SplitDecision.Label.Failure, Unknown);
        assertEquals(counter.toMetrics(null).toString(),
                counterExpected.toMetrics(null).toString());
    }

    @Test
    public void testTimeout() throws IllegalAccessException {
        var counter = new SplitCounter();
        SplitManager manager = new SplitManager(counter);
        var decision = new SplitDecision();
        CompletableFuture<Void> future =
                manager.waitAsync(CompletableFuture.completedFuture(null),
                        bundle, decision, 3, TimeUnit.SECONDS);
        var inFlightUnloadRequests = getinFlightUnloadRequests(manager);

        assertEquals(inFlightUnloadRequests.size(), 1);

        try {
            future.get();
            fail();
        } catch (Exception ex) {
            assertTrue(ex.getCause() instanceof TimeoutException);
        }

        assertEquals(inFlightUnloadRequests.size(), 0);
        var counterExpected = new SplitCounter();
        counterExpected.update(SplitDecision.Label.Failure, Unknown);
        assertEquals(counter.toMetrics(null).toString(),
                counterExpected.toMetrics(null).toString());
    }

    @Test
    public void testSuccess() throws IllegalAccessException, ExecutionException, InterruptedException {
        var counter = new SplitCounter();
        SplitManager manager = new SplitManager(counter);
        var counterExpected = new SplitCounter();
        var decision = new SplitDecision();
        decision.succeed(Sessions);
        CompletableFuture<Void> future =
                manager.waitAsync(CompletableFuture.completedFuture(null),
                        bundle, decision, 5, TimeUnit.SECONDS);
        var inFlightUnloadRequests = getinFlightUnloadRequests(manager);

        assertEquals(inFlightUnloadRequests.size(), 1);

        manager.handleEvent(bundle,
                new ServiceUnitStateData(ServiceUnitState.Assigning, dstBroker, VERSION_ID_INIT), null);
        assertEquals(inFlightUnloadRequests.size(), 1);

        manager.handleEvent(bundle,
                new ServiceUnitStateData(ServiceUnitState.Splitting, dstBroker, VERSION_ID_INIT), null);
        assertEquals(inFlightUnloadRequests.size(), 1);

        manager.handleEvent(bundle,
                new ServiceUnitStateData(ServiceUnitState.Releasing, dstBroker, VERSION_ID_INIT), null);
        assertEquals(inFlightUnloadRequests.size(), 1);

        manager.handleEvent(bundle,
                new ServiceUnitStateData(ServiceUnitState.Free, dstBroker, VERSION_ID_INIT), null);
        assertEquals(inFlightUnloadRequests.size(), 1);

        manager.handleEvent(bundle,
                new ServiceUnitStateData(ServiceUnitState.Deleted, dstBroker, VERSION_ID_INIT), null);
        assertEquals(inFlightUnloadRequests.size(), 1);

        manager.handleEvent(bundle,
                new ServiceUnitStateData(ServiceUnitState.Owned, dstBroker, VERSION_ID_INIT), null);
        assertEquals(inFlightUnloadRequests.size(), 1);

        // Success with Init state.
        manager.handleEvent(bundle,
                new ServiceUnitStateData(ServiceUnitState.Init, dstBroker, VERSION_ID_INIT), null);
        assertEquals(inFlightUnloadRequests.size(), 0);
        counterExpected.update(SplitDecision.Label.Success, Sessions);
        assertEquals(counter.toMetrics(null).toString(),
                counterExpected.toMetrics(null).toString());

        future.get();
    }

    @Test
    public void testFailedStage() throws IllegalAccessException {
        var counter = new SplitCounter();
        SplitManager manager = new SplitManager(counter);
        var decision = new SplitDecision();
        CompletableFuture<Void> future =
                manager.waitAsync(CompletableFuture.completedFuture(null),
                        bundle, decision, 5, TimeUnit.SECONDS);
        var inFlightUnloadRequests = getinFlightUnloadRequests(manager);

        assertEquals(inFlightUnloadRequests.size(), 1);

        manager.handleEvent(bundle,
                new ServiceUnitStateData(ServiceUnitState.Owned, dstBroker, VERSION_ID_INIT),
                new IllegalStateException("Failed stage."));

        try {
            future.get();
            fail();
        } catch (Exception ex) {
            assertTrue(ex.getCause() instanceof IllegalStateException);
            assertEquals(ex.getCause().getMessage(), "Failed stage.");
        }

        assertEquals(inFlightUnloadRequests.size(), 0);
        var counterExpected = new SplitCounter();
        counterExpected.update(SplitDecision.Label.Failure, Unknown);
        assertEquals(counter.toMetrics(null).toString(),
                counterExpected.toMetrics(null).toString());
    }

    @Test
    public void testClose() throws IllegalAccessException {
        SplitManager manager = new SplitManager(new SplitCounter());
        var decision = new SplitDecision();
        CompletableFuture<Void> future =
                manager.waitAsync(CompletableFuture.completedFuture(null),
                        bundle, decision, 5, TimeUnit.SECONDS);
        var inFlightUnloadRequests = getinFlightUnloadRequests(manager);
        assertEquals(inFlightUnloadRequests.size(), 1);
        manager.close();
        assertEquals(inFlightUnloadRequests.size(), 0);

        try {
            future.get();
            fail();
        } catch (Exception ex) {
            assertTrue(ex.getCause() instanceof IllegalStateException);
        }
    }

    private Map<String, CompletableFuture<Void>> getinFlightUnloadRequests(SplitManager manager)
            throws IllegalAccessException {
        var inFlightUnloadRequest =
                (Map<String, CompletableFuture<Void>>) FieldUtils.readField(manager, "inFlightSplitRequests", true);

        return inFlightUnloadRequest;
    }

}
