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
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Label.Failure;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Label.Success;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Reason.Admin;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Reason.Unknown;
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
import org.apache.pulsar.broker.loadbalance.extensions.models.Unload;
import org.apache.pulsar.broker.loadbalance.extensions.models.UnloadCounter;
import org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision;
import org.apache.pulsar.common.util.FutureUtil;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker")
public class UnloadManagerTest {

    @Test
    public void testEventPubFutureHasException() {
        UnloadCounter counter = new UnloadCounter();
        UnloadManager manager = new UnloadManager(counter, "mockBrokerId");
        var unloadDecision =
                new UnloadDecision(new Unload("broker-1", "bundle-1"), Success, Admin);
        CompletableFuture<Void> future =
                manager.waitAsync(FutureUtil.failedFuture(new Exception("test")),
                        "bundle-1", unloadDecision, 10, TimeUnit.SECONDS);

        assertTrue(future.isCompletedExceptionally());
        try {
            future.get();
            fail();
        } catch (Exception ex) {
            assertEquals(ex.getCause().getMessage(), "test");
        }
        assertEquals(counter.getBreakdownCounters().get(Failure).get(Unknown).get(), 1);
    }

    @Test
    public void testTimeout() throws IllegalAccessException {
        UnloadCounter counter = new UnloadCounter();
        UnloadManager manager = new UnloadManager(counter, "mockBrokerId");
        var unloadDecision =
                new UnloadDecision(new Unload("broker-1", "bundle-1"), Success, Admin);
        CompletableFuture<Void> future =
                manager.waitAsync(CompletableFuture.completedFuture(null),
                        "bundle-1", unloadDecision, 3, TimeUnit.SECONDS);
        Map<String, CompletableFuture<Void>> inFlightUnloadRequestMap = getInFlightUnloadRequestMap(manager);

        assertEquals(inFlightUnloadRequestMap.size(), 1);

        try {
            future.get();
            fail();
        } catch (Exception ex) {
            assertTrue(ex.getCause() instanceof TimeoutException);
        }

        assertEquals(inFlightUnloadRequestMap.size(), 0);
        assertEquals(counter.getBreakdownCounters().get(Failure).get(Unknown).get(), 1);
    }

    @Test
    public void testSuccess() throws IllegalAccessException, ExecutionException, InterruptedException {
        UnloadCounter counter = new UnloadCounter();
        UnloadManager manager = new UnloadManager(counter, "mockBrokerId");
        String dstBroker = "broker-2";
        String srcBroker = "broker-1";
        String bundle = "bundle-1";
        var unloadDecision =
                new UnloadDecision(new Unload(srcBroker, bundle), Success, Admin);
        CompletableFuture<Void> future =
                manager.waitAsync(CompletableFuture.completedFuture(null),
                        bundle, unloadDecision, 5, TimeUnit.SECONDS);
        Map<String, CompletableFuture<Void>> inFlightUnloadRequestMap = getInFlightUnloadRequestMap(manager);

        assertEquals(inFlightUnloadRequestMap.size(), 1);

        manager.handleEvent(bundle,
                new ServiceUnitStateData(ServiceUnitState.Assigning, null, srcBroker, VERSION_ID_INIT), null);
        assertEquals(inFlightUnloadRequestMap.size(), 1);

        manager.handleEvent(bundle,
                new ServiceUnitStateData(ServiceUnitState.Deleted, null, srcBroker, VERSION_ID_INIT), null);
        assertEquals(inFlightUnloadRequestMap.size(), 1);

        manager.handleEvent(bundle,
                new ServiceUnitStateData(ServiceUnitState.Splitting, null, srcBroker, VERSION_ID_INIT), null);
        assertEquals(inFlightUnloadRequestMap.size(), 1);

        manager.handleEvent(bundle,
                new ServiceUnitStateData(ServiceUnitState.Releasing, null, srcBroker, VERSION_ID_INIT), null);
        assertEquals(inFlightUnloadRequestMap.size(), 1);

        manager.handleEvent(bundle,
                new ServiceUnitStateData(ServiceUnitState.Free, null, srcBroker, true, VERSION_ID_INIT), null);
        assertEquals(inFlightUnloadRequestMap.size(), 1);

        // Success with Init state.
        manager.handleEvent(bundle, null, null);
        assertEquals(inFlightUnloadRequestMap.size(), 0);
        future.get();
        assertEquals(counter.getBreakdownCounters().get(Success).get(Admin).get(), 1);

        // Success with Owned state.
        future = manager.waitAsync(CompletableFuture.completedFuture(null),
                bundle, unloadDecision, 5, TimeUnit.SECONDS);
        inFlightUnloadRequestMap = getInFlightUnloadRequestMap(manager);
        assertEquals(inFlightUnloadRequestMap.size(), 1);
        manager.handleEvent(bundle,
                new ServiceUnitStateData(ServiceUnitState.Owned, dstBroker, null, VERSION_ID_INIT), null);
        assertEquals(inFlightUnloadRequestMap.size(), 1);
        manager.handleEvent(bundle,
                new ServiceUnitStateData(ServiceUnitState.Owned, dstBroker, srcBroker, VERSION_ID_INIT), null);
        assertEquals(inFlightUnloadRequestMap.size(), 0);
        future.get();
        assertEquals(counter.getBreakdownCounters().get(Success).get(Admin).get(), 2);

        // Success with Free state.
        future = manager.waitAsync(CompletableFuture.completedFuture(null),
                bundle, unloadDecision, 5, TimeUnit.SECONDS);
        inFlightUnloadRequestMap = getInFlightUnloadRequestMap(manager);
        assertEquals(inFlightUnloadRequestMap.size(), 1);
        manager.handleEvent(bundle,
                new ServiceUnitStateData(ServiceUnitState.Free, dstBroker, srcBroker, true, VERSION_ID_INIT), null);
        assertEquals(inFlightUnloadRequestMap.size(), 1);
        manager.handleEvent(bundle,
                new ServiceUnitStateData(ServiceUnitState.Free, dstBroker, srcBroker, false, VERSION_ID_INIT), null);
        assertEquals(inFlightUnloadRequestMap.size(), 0);
        future.get();
        assertEquals(counter.getBreakdownCounters().get(Success).get(Admin).get(), 3);


    }

    @Test
    public void testFailedStage() throws IllegalAccessException {
        UnloadCounter counter = new UnloadCounter();
        UnloadManager manager = new UnloadManager(counter, "mockBrokerId");
        var unloadDecision =
                new UnloadDecision(new Unload("broker-1", "bundle-1"), Success, Admin);
        CompletableFuture<Void> future =
                manager.waitAsync(CompletableFuture.completedFuture(null),
                        "bundle-1", unloadDecision, 5, TimeUnit.SECONDS);
        Map<String, CompletableFuture<Void>> inFlightUnloadRequestMap = getInFlightUnloadRequestMap(manager);

        assertEquals(inFlightUnloadRequestMap.size(), 1);

        manager.handleEvent("bundle-1",
                new ServiceUnitStateData(ServiceUnitState.Owned, null, "broker-1", VERSION_ID_INIT),
                new IllegalStateException("Failed stage."));

        try {
            future.get();
            fail();
        } catch (Exception ex) {
            assertTrue(ex.getCause() instanceof IllegalStateException);
            assertEquals(ex.getCause().getMessage(), "Failed stage.");
        }

        assertEquals(inFlightUnloadRequestMap.size(), 0);
        assertEquals(counter.getBreakdownCounters().get(Failure).get(Unknown).get(), 1);
    }

    @Test
    public void testClose() throws IllegalAccessException {
        UnloadCounter counter = new UnloadCounter();
        UnloadManager manager = new UnloadManager(counter, "mockBrokerId");
        var unloadDecision =
                new UnloadDecision(new Unload("broker-1", "bundle-1"), Success, Admin);
        CompletableFuture<Void> future =
                manager.waitAsync(CompletableFuture.completedFuture(null),
                        "bundle-1", unloadDecision,5, TimeUnit.SECONDS);
        Map<String, CompletableFuture<Void>> inFlightUnloadRequestMap = getInFlightUnloadRequestMap(manager);
        assertEquals(inFlightUnloadRequestMap.size(), 1);
        manager.close();
        assertEquals(inFlightUnloadRequestMap.size(), 0);

        try {
            future.get();
            fail();
        } catch (Exception ex) {
            assertTrue(ex.getCause() instanceof IllegalStateException);
        }
        assertEquals(counter.getBreakdownCounters().get(Failure).get(Unknown).get(), 1);
    }

    private Map<String, CompletableFuture<Void>> getInFlightUnloadRequestMap(UnloadManager manager)
            throws IllegalAccessException {
        Map<String, CompletableFuture<Void>> inFlightUnloadRequest =
                (Map<String, CompletableFuture<Void>>) FieldUtils.readField(manager, "inFlightUnloadRequest", true);

        return inFlightUnloadRequest;
    }

}
