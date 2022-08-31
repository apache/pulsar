/**
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
package org.apache.pulsar.broker;

import static org.mockito.Mockito.mock;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.impl.AbstractMetadataStore;
import org.apache.pulsar.metadata.impl.ZKMetadataStore;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

@Slf4j
public class PulsarServiceMockSupport {

    /**
     * see: https://github.com/apache/pulsar/pull/16821.
     * While executing "doReturn(pulsarResources).when(pulsar).getPulsarResources()", Meta Store Thread also accesses
     * variable PulsarService.getPulsarResources() asynchronously in logic: "notification by zk-watcher".
     * So execute mock-cmd in meta-thread (The executor of MetaStore is a single thread pool executor, so all things
     * will be thread safety).
     * Note: If the MetaStore's executor is no longer single-threaded, should mock as single-threaded if you need to
     * execute this method.
     */
    public static void mockPulsarServiceProps(final PulsarService pulsarService, Runnable mockTask)
            throws ExecutionException, InterruptedException, TimeoutException {
        final CompletableFuture<Void> mockGetPulsarResourceFuture = new CompletableFuture<>();
        MetadataStoreExtended metadataStoreExtended = pulsarService.getLocalMetadataStore();
        if (metadataStoreExtended instanceof AbstractMetadataStore){
            AbstractMetadataStore abstractMetadataStore = (AbstractMetadataStore) metadataStoreExtended;
            abstractMetadataStore.execute(() -> {
                mockTask.run();
                mockGetPulsarResourceFuture.complete(null);
            }, mock(CompletableFuture.class));
            try {
                mockGetPulsarResourceFuture.get(1, TimeUnit.SECONDS);
            } catch (TimeoutException timeoutException){
                mockTask.run();
            }
        } else {
            mockTask.run();
        }
    }

    @Test
    public void testMockMetaStore() throws Exception{
        AtomicInteger integer = new AtomicInteger();
        PulsarService pulsarService = Mockito.mock(PulsarService.class);
        Mockito.when(pulsarService.getLocalMetadataStore()).thenReturn(Mockito.mock(ZKMetadataStore.class));
        mockPulsarServiceProps(pulsarService, () -> integer.incrementAndGet());
        Assert.assertEquals(integer.get(), 1);
    }
}
