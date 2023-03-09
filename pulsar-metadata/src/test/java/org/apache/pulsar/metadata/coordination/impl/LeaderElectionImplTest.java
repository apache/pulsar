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
package org.apache.pulsar.metadata.coordination.impl;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import lombok.Cleanup;
import org.apache.pulsar.metadata.BaseMetadataStoreTest;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.coordination.CoordinationService;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.testng.annotations.Test;

public class LeaderElectionImplTest extends BaseMetadataStoreTest {

    @Test(dataProvider = "impl", timeOut = 10000)
    public void validateDeadLock(String provider, Supplier<String> urlSupplier)
            throws Exception {
        if (provider.equals("Memory") || provider.equals("RocksDB")) {
            // There are no multiple sessions for the local memory provider
            return;
        }

        @Cleanup
        MetadataStoreExtended store = MetadataStoreExtended.create(urlSupplier.get(),
                MetadataStoreConfig.builder().build());

        String path = newKey();

        @Cleanup
        CoordinationService cs = new CoordinationServiceImpl(store);

        @Cleanup
        LeaderElectionImpl<String> le = (LeaderElectionImpl<String>) cs.getLeaderElection(String.class,
                path, __ -> {
                });
        final CompletableFuture<Void> blockFuture = new CompletableFuture<>();
        // simulate handleSessionNotification method logic
        le.getSchedulerExecutor().execute(() -> {
            try {
                le.elect("test-2").join();
                blockFuture.complete(null);
            } catch (Throwable ex) {
                blockFuture.completeExceptionally(ex);
            }
        });
        blockFuture.join();
    }
}
