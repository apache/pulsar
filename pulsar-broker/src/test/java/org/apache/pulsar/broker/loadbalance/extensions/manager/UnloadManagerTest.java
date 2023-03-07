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
import org.apache.pulsar.common.util.FutureUtil;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker")
public class UnloadManagerTest {

    @Test
    public void testEventPubFutureHasException() {
        UnloadManager manager = new UnloadManager();
        CompletableFuture<Void> future =
                manager.waitAsync(FutureUtil.failedFuture(new Exception("test")),
                        "bundle-1", 10, TimeUnit.SECONDS);

        assertTrue(future.isCompletedExceptionally());
        try {
            future.get();
            fail();
        } catch (Exception ex) {
            assertEquals(ex.getCause().getMessage(), "test");
        }
    }

    @Test
    public void testTimeout() throws IllegalAccessException {
        UnloadManager manager = new UnloadManager();
        CompletableFuture<Void> future =
                manager.waitAsync(CompletableFuture.completedFuture(null),
                        "bundle-1", 3, TimeUnit.SECONDS);
        Map<String, CompletableFuture<Void>> inFlightUnloadRequestMap = getInFlightUnloadRequestMap(manager);

        assertEquals(inFlightUnloadRequestMap.size(), 1);

        try {
            future.get();
            fail();
        } catch (Exception ex) {
            assertTrue(ex.getCause() instanceof TimeoutException);
        }

        assertEquals(inFlightUnloadRequestMap.size(), 0);
    }

    @Test
    public void testSuccess() throws IllegalAccessException, ExecutionException, InterruptedException {
        UnloadManager manager = new UnloadManager();
        CompletableFuture<Void> future =
                manager.waitAsync(CompletableFuture.completedFuture(null),
                        "bundle-1", 5, TimeUnit.SECONDS);
        Map<String, CompletableFuture<Void>> inFlightUnloadRequestMap = getInFlightUnloadRequestMap(manager);

        assertEquals(inFlightUnloadRequestMap.size(), 1);

        manager.handleEvent("bundle-1",
                new ServiceUnitStateData(ServiceUnitState.Assigning, "broker-1", VERSION_ID_INIT), null);
        assertEquals(inFlightUnloadRequestMap.size(), 1);

        manager.handleEvent("bundle-1",
                new ServiceUnitStateData(ServiceUnitState.Deleted, "broker-1", VERSION_ID_INIT), null);
        assertEquals(inFlightUnloadRequestMap.size(), 1);

        manager.handleEvent("bundle-1",
                new ServiceUnitStateData(ServiceUnitState.Splitting, "broker-1", VERSION_ID_INIT), null);
        assertEquals(inFlightUnloadRequestMap.size(), 1);

        manager.handleEvent("bundle-1",
                new ServiceUnitStateData(ServiceUnitState.Releasing, "broker-1", VERSION_ID_INIT), null);
        assertEquals(inFlightUnloadRequestMap.size(), 1);

        manager.handleEvent("bundle-1",
                new ServiceUnitStateData(ServiceUnitState.Init, "broker-1", VERSION_ID_INIT), null);
        assertEquals(inFlightUnloadRequestMap.size(), 1);

        manager.handleEvent("bundle-1",
                new ServiceUnitStateData(ServiceUnitState.Free, "broker-1", VERSION_ID_INIT), null);
        assertEquals(inFlightUnloadRequestMap.size(), 0);
        future.get();

        // Success with Owned state.
        future = manager.waitAsync(CompletableFuture.completedFuture(null),
                "bundle-1", 5, TimeUnit.SECONDS);
        inFlightUnloadRequestMap = getInFlightUnloadRequestMap(manager);

        assertEquals(inFlightUnloadRequestMap.size(), 1);

        manager.handleEvent("bundle-1",
                new ServiceUnitStateData(ServiceUnitState.Owned, "broker-1", VERSION_ID_INIT), null);
        assertEquals(inFlightUnloadRequestMap.size(), 0);
        future.get();
    }

    @Test
    public void testFailedStage() throws IllegalAccessException {
        UnloadManager manager = new UnloadManager();
        CompletableFuture<Void> future =
                manager.waitAsync(CompletableFuture.completedFuture(null),
                        "bundle-1", 5, TimeUnit.SECONDS);
        Map<String, CompletableFuture<Void>> inFlightUnloadRequestMap = getInFlightUnloadRequestMap(manager);

        assertEquals(inFlightUnloadRequestMap.size(), 1);

        manager.handleEvent("bundle-1",
                new ServiceUnitStateData(ServiceUnitState.Owned, "broker-1", VERSION_ID_INIT),
                new IllegalStateException("Failed stage."));

        try {
            future.get();
            fail();
        } catch (Exception ex) {
            assertTrue(ex.getCause() instanceof IllegalStateException);
            assertEquals(ex.getCause().getMessage(), "Failed stage.");
        }

        assertEquals(inFlightUnloadRequestMap.size(), 0);
    }

    @Test
    public void testClose() throws IllegalAccessException {
        UnloadManager manager = new UnloadManager();
        CompletableFuture<Void> future =
                manager.waitAsync(CompletableFuture.completedFuture(null),
                        "bundle-1", 5, TimeUnit.SECONDS);
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
    }

    private Map<String, CompletableFuture<Void>> getInFlightUnloadRequestMap(UnloadManager manager)
            throws IllegalAccessException {
        Map<String, CompletableFuture<Void>> inFlightUnloadRequest =
                (Map<String, CompletableFuture<Void>>) FieldUtils.readField(manager, "inFlightUnloadRequest", true);

        return inFlightUnloadRequest;
    }

}
