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
package org.apache.pulsar.broker.service;

import static org.testng.Assert.assertEquals;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.Cleanup;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.coordination.CoordinationService;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.coordination.impl.CoordinationServiceImpl;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class DistributedIdGeneratorTest {

    private MetadataStoreExtended store;
    private CoordinationService coordinationService;

    @BeforeMethod(alwaysRun = true)
    public void setup() throws Exception {
        store  = MetadataStoreExtended.create("memory:local", MetadataStoreConfig.builder().build());
        coordinationService = new CoordinationServiceImpl(store);
    }

    @AfterMethod(alwaysRun = true)
    public void teardown() throws Exception {
        coordinationService.close();
        store.close();
    }

    @Test
    public void simple() throws Exception {
        DistributedIdGenerator gen1 = new DistributedIdGenerator(coordinationService, "/my/test/simple", "p");

        assertEquals(gen1.getNextId(), "p-0-0");
        assertEquals(gen1.getNextId(), "p-0-1");
        assertEquals(gen1.getNextId(), "p-0-2");
        assertEquals(gen1.getNextId(), "p-0-3");

        DistributedIdGenerator gen2 = new DistributedIdGenerator(coordinationService, "/my/test/simple", "p");
        assertEquals(gen2.getNextId(), "p-1-0");
        assertEquals(gen2.getNextId(), "p-1-1");

        assertEquals(gen1.getNextId(), "p-0-4");
        assertEquals(gen2.getNextId(), "p-1-2");
    }

    /**
     * Use multiple threads to generate many Id. Ensure no holes and no dups in the sequence
     */
    @Test
    public void concurrent() throws Exception {
        int Threads = 10;
        int Iterations = 100;

        CyclicBarrier barrier = new CyclicBarrier(Threads);
        CountDownLatch counter = new CountDownLatch(Threads);
        @Cleanup("shutdownNow")
        ExecutorService executor = Executors.newCachedThreadPool();

        List<String> results = Collections.synchronizedList(Lists.newArrayList());

        for (int i = 0; i < Threads; i++) {
            executor.execute(() -> {
                try {
                    DistributedIdGenerator gen = new DistributedIdGenerator(coordinationService, "/my/test/concurrent", "prefix");

                    barrier.await();

                    for (int j = 0; j < Iterations; j++) {
                        results.add(gen.getNextId());
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    counter.countDown();
                }
            });
        }

        counter.await();

        assertEquals(results.size(), Threads * Iterations);

        // Check the list contains no duplicates
        Set<String> set = Sets.newHashSet(results);
        assertEquals(set.size(), results.size());
    }

    @Test
    public void invalidZnode() throws Exception {
        store.put("/my/test/invalid", "invalid-number".getBytes(), Optional.of(-1L));

        DistributedIdGenerator gen = new DistributedIdGenerator(coordinationService, "/my/test/invalid", "p");

        // It should not get exception if content is there
        assertEquals(gen.getNextId(), "p-0-0");
    }
}
