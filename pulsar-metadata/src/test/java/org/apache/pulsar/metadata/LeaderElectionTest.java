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
package org.apache.pulsar.metadata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import lombok.Cleanup;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.metadata.api.MetadataCache;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.coordination.CoordinationService;
import org.apache.pulsar.metadata.api.coordination.LeaderElection;
import org.apache.pulsar.metadata.api.coordination.LeaderElectionState;
import org.apache.pulsar.metadata.api.extended.CreateOption;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.coordination.impl.CoordinationServiceImpl;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

public class LeaderElectionTest extends BaseMetadataStoreTest {

    @Test(dataProvider = "impl")
    public void basicTest(String provider, Supplier<String> urlSupplier) throws Exception {
        @Cleanup
        MetadataStoreExtended store = MetadataStoreExtended.create(urlSupplier.get(),
                MetadataStoreConfig.builder().fsyncEnable(false).build());

        @Cleanup
        CoordinationService coordinationService = new CoordinationServiceImpl(store);

        MetadataCache<String> cache = store.getMetadataCache(String.class);

        BlockingQueue<LeaderElectionState> notifications = new LinkedBlockingDeque<>();

        @Cleanup
        LeaderElection<String> leaderElection = coordinationService.getLeaderElection(String.class,
                "/my/leader-election", t -> {
                    notifications.add(t);
                });

        assertEquals(cache.get("/my/leader-election").join(), Optional.empty());

        LeaderElectionState les = leaderElection.elect("test-1").join();
        assertEquals(les, LeaderElectionState.Leading);

        assertEquals(notifications.poll(3, TimeUnit.SECONDS), LeaderElectionState.Leading);

        assertEquals(cache.get("/my/leader-election").join(), Optional.of("test-1"));

        leaderElection.close();

        assertEquals(leaderElection.getState(), LeaderElectionState.NoLeader);

        assertEquals(cache.get("/my/leader-election").join(), Optional.empty());
    }

    @Test(dataProvider = "impl")
    public void multipleMembers(String provider, Supplier<String> urlSupplier) throws Exception {
        if (provider.equals("Memory") || provider.equals("RocksDB")) {
            // There are no multiple session in local mem provider
            return;
        }

        @Cleanup
        MetadataStoreExtended store1 = MetadataStoreExtended.create(urlSupplier.get(),
                MetadataStoreConfig.builder().build());
        @Cleanup
        MetadataStoreExtended store2 = MetadataStoreExtended.create(urlSupplier.get(),
                MetadataStoreConfig.builder().build());


        @Cleanup
        CoordinationService cs1 = new CoordinationServiceImpl(store1);

        BlockingQueue<LeaderElectionState> n1 = new LinkedBlockingDeque<>();

        @Cleanup
        LeaderElection<String> le1 = cs1.getLeaderElection(String.class,
                "/my/leader-election", t -> {
                    n1.add(t);
                });

        @Cleanup
        CoordinationService cs2 = new CoordinationServiceImpl(store2);

        BlockingQueue<LeaderElectionState> n2 = new LinkedBlockingDeque<>();

        @Cleanup
        LeaderElection<String> le2 = cs2.getLeaderElection(String.class,
                "/my/leader-election", t -> {
                    n2.add(t);
                });

        LeaderElectionState les1 = le1.elect("test-1").join();
        assertEquals(les1, LeaderElectionState.Leading);
        assertEqualsAndRetry(() -> le1.getLeaderValueIfPresent(), Optional.of("test-1"), Optional.empty());
        assertEquals(le1.getLeaderValue().join(), Optional.of("test-1"));
        assertEquals(n1.poll(3, TimeUnit.SECONDS), LeaderElectionState.Leading);

        LeaderElectionState les2 = le2.elect("test-2").join();
        assertEquals(les2, LeaderElectionState.Following);
        assertEquals(le2.getLeaderValue().join(), Optional.of("test-1"));
        assertEqualsAndRetry(() -> le2.getLeaderValueIfPresent(), Optional.of("test-1"), Optional.empty());
        assertEquals(n2.poll(3, TimeUnit.SECONDS), LeaderElectionState.Following);

        le1.close();

        assertEquals(n2.poll(3, TimeUnit.SECONDS), LeaderElectionState.Leading);
        assertEquals(le2.getState(), LeaderElectionState.Leading);
        assertEqualsAndRetry(() -> le2.getLeaderValueIfPresent(), Optional.of("test-2"), Optional.empty());
        assertEquals(le2.getLeaderValue().join(), Optional.of("test-2"));
    }

    @Test(dataProvider = "impl")
    public void leaderNodeIsDeletedExternally(String provider, Supplier<String> urlSupplier) throws Exception {
        @Cleanup
        MetadataStoreExtended store = MetadataStoreExtended.create(urlSupplier.get(),
                MetadataStoreConfig.builder().fsyncEnable(false).build());

        @Cleanup
        CoordinationService coordinationService = new CoordinationServiceImpl(store);

        BlockingQueue<LeaderElectionState> notifications = new LinkedBlockingDeque<>();

        @Cleanup
        LeaderElection<String> leaderElection = coordinationService.getLeaderElection(String.class,
                "/my/leader-election", t -> {
                    notifications.add(t);
                });

        LeaderElectionState les = leaderElection.elect("test-1").join();
        assertEquals(les, LeaderElectionState.Leading);

        assertEquals(notifications.poll(3, TimeUnit.SECONDS), LeaderElectionState.Leading);

        store.delete("/my/leader-election", Optional.empty()).join();

        assertEquals(notifications.poll(3, TimeUnit.SECONDS), LeaderElectionState.Leading);
        assertEquals(les, LeaderElectionState.Leading);
    }

    @Test(dataProvider = "impl")
    public void closeAll(String provider, Supplier<String> urlSupplier) throws Exception {
        @Cleanup
        MetadataStoreExtended store = MetadataStoreExtended.create(urlSupplier.get(),
                MetadataStoreConfig.builder().fsyncEnable(false).build());
        MetadataCache<String> cache = store.getMetadataCache(String.class);

        CoordinationService cs = new CoordinationServiceImpl(store);

        LeaderElection<String> le1 = cs.getLeaderElection(String.class,
                "/my/leader-election-1", t -> {
                });

        LeaderElection<String> le2 = cs.getLeaderElection(String.class,
                "/my/leader-election-2", t -> {
                });

        LeaderElectionState les1 = le1.elect("test-1").join();
        assertEquals(les1, LeaderElectionState.Leading);

        LeaderElectionState les2 = le2.elect("test-2").join();
        assertEquals(les2, LeaderElectionState.Leading);

        cs.close();

        assertEquals(cache.get("/my/leader-election-1").join(), Optional.empty());
        assertEquals(cache.get("/my/leader-election-2").join(), Optional.empty());
    }


    @Test(dataProvider = "impl")
    public void revalidateLeaderWithinSameSession(String provider, Supplier<String> urlSupplier) throws Exception {
        @Cleanup
        MetadataStoreExtended store = MetadataStoreExtended.create(urlSupplier.get(),
                MetadataStoreConfig.builder().fsyncEnable(false).build());

        String path = newKey();

        @Cleanup
        CoordinationService cs = new CoordinationServiceImpl(store);

        @Cleanup
        LeaderElection<String> le = cs.getLeaderElection(String.class,
                path, __ -> {
                });

        store.put(path, ObjectMapperFactory.getMapper().writer().writeValueAsBytes("test-1"), Optional.of(-1L),
                EnumSet.of(CreateOption.Ephemeral)).join();

        LeaderElectionState les = le.elect("test-2").join();
        assertEquals(les, LeaderElectionState.Leading);
        assertEquals(le.getLeaderValue().join(), Optional.of("test-2"));
        assertEqualsAndRetry(() -> le.getLeaderValueIfPresent(), Optional.of("test-2"), Optional.empty());
    }

    @Test(dataProvider = "impl")
    public void revalidateLeaderWithDifferentSessionsSameValue(String provider, Supplier<String> urlSupplier)
            throws Exception {
        if (provider.equals("Memory") || provider.equals("RocksDB")) {
            // There are no multiple sessions for the local memory provider
            return;
        }

        @Cleanup
        MetadataStoreExtended store = MetadataStoreExtended.create(urlSupplier.get(),
                MetadataStoreConfig.builder().build());

        @Cleanup
        MetadataStoreExtended store2 = MetadataStoreExtended.create(urlSupplier.get(),
                MetadataStoreConfig.builder().build());

        String path = newKey();

        @Cleanup
        CoordinationService cs = new CoordinationServiceImpl(store);

        @Cleanup
        LeaderElection<String> le = cs.getLeaderElection(String.class,
                path, __ -> {
                });

        store2.put(path, ObjectMapperFactory.getMapper().writer().writeValueAsBytes("test-1"), Optional.of(-1L),
                EnumSet.of(CreateOption.Ephemeral)).join();

        LeaderElectionState les = le.elect("test-1").join();
        assertEquals(les, LeaderElectionState.Leading);
        assertEquals(le.getLeaderValue().join(), Optional.of("test-1"));
        assertEqualsAndRetry(() -> le.getLeaderValueIfPresent(), Optional.of("test-1"), Optional.empty());
    }


    @Test(dataProvider = "impl")
    public void revalidateLeaderWithDifferentSessionsDifferentValue(String provider, Supplier<String> urlSupplier)
            throws Exception {
        if (provider.equals("Memory") || provider.equals("RocksDB")) {
            // There are no multiple sessions for the local memory provider
            return;
        }

        @Cleanup
        MetadataStoreExtended store = MetadataStoreExtended.create(urlSupplier.get(),
                MetadataStoreConfig.builder().build());

        @Cleanup
        MetadataStoreExtended store2 = MetadataStoreExtended.create(urlSupplier.get(),
                MetadataStoreConfig.builder().build());

        String path = newKey();

        @Cleanup
        CoordinationService cs = new CoordinationServiceImpl(store);

        @Cleanup
        LeaderElection<String> le = cs.getLeaderElection(String.class,
                path, __ -> {
                });

        store2.put(path, ObjectMapperFactory.getMapper().writer().writeValueAsBytes("test-1"), Optional.of(-1L),
                EnumSet.of(CreateOption.Ephemeral)).join();

        LeaderElectionState les = le.elect("test-2").join();
        assertEquals(les, LeaderElectionState.Following);
        assertEquals(le.getLeaderValue().join(), Optional.of("test-1"));
        assertEqualsAndRetry(() -> le.getLeaderValueIfPresent(), Optional.of("test-1"), Optional.empty());
    }

    @Test(dataProvider = "impl", timeOut = 30000)
    public void readsDoNotObserveEmptyLeaderDuringReElection(String provider, Supplier<String> urlSupplier)
            throws Exception {
        @Cleanup
        MetadataStoreExtended store = MetadataStoreExtended.create(urlSupplier.get(),
                MetadataStoreConfig.builder().fsyncEnable(false).build());

        String path = newKey();

        @Cleanup
        CoordinationService cs = new CoordinationServiceImpl(store);

        @Cleanup
        LeaderElection<String> le = cs.getLeaderElection(String.class, path, __ -> {
        });

        assertEquals(le.elect("test-1").join(), LeaderElectionState.Leading);

        // Externally delete the leader node: the instance re-elects itself. An authoritative read
        // issued during the churn either returns the last settled value or waits for the
        // re-election to settle — it never observes an empty leader.
        store.delete(path, Optional.empty()).join();
        assertEquals(le.getLeaderValue().join(), Optional.of("test-1"));

        Awaitility.await().untilAsserted(() -> {
            assertEquals(le.getState(), LeaderElectionState.Leading);
            assertEquals(le.getLeaderValueIfPresent(), Optional.of("test-1"));
        });
    }

    @Test(dataProvider = "zkImpls", timeOut = 30000)
    public void followerReadsResolveToTheNewLeaderAfterHandoff(String provider, Supplier<String> urlSupplier)
            throws Exception {
        @Cleanup
        MetadataStoreExtended store1 = MetadataStoreExtended.create(urlSupplier.get(),
                MetadataStoreConfig.builder().build());
        @Cleanup
        MetadataStoreExtended store2 = MetadataStoreExtended.create(urlSupplier.get(),
                MetadataStoreConfig.builder().build());

        String path = newKey();

        @Cleanup
        CoordinationService cs1 = new CoordinationServiceImpl(store1);
        @Cleanup
        CoordinationService cs2 = new CoordinationServiceImpl(store2);

        @Cleanup
        LeaderElection<String> le1 = cs1.getLeaderElection(String.class, path, __ -> {
        });
        @Cleanup
        LeaderElection<String> le2 = cs2.getLeaderElection(String.class, path, __ -> {
        });

        assertEquals(le1.elect("test-1").join(), LeaderElectionState.Leading);
        assertEquals(le2.elect("test-2").join(), LeaderElectionState.Following);
        assertEquals(le2.getLeaderValue().join(), Optional.of("test-1"));

        // The leader hands off: le2 re-elects itself. Authoritative reads during the handoff
        // return one of the settled leader values and converge to the new leader, but never
        // observe an empty leader.
        le1.close();
        List<Optional<String>> observed = new CopyOnWriteArrayList<>();
        Awaitility.await().untilAsserted(() -> {
            Optional<String> leader = le2.getLeaderValue().join();
            observed.add(leader);
            assertEquals(leader, Optional.of("test-2"));
        });
        assertThat(observed)
                .as("authoritative reads during the leadership handoff")
                .doesNotContain(Optional.empty());
    }

    @Test(dataProvider = "impl", timeOut = 30000)
    public void closedLeaderReportsEmptyLeader(String provider, Supplier<String> urlSupplier) throws Exception {
        @Cleanup
        MetadataStoreExtended store = MetadataStoreExtended.create(urlSupplier.get(),
                MetadataStoreConfig.builder().fsyncEnable(false).build());

        String path = newKey();

        @Cleanup
        CoordinationService cs = new CoordinationServiceImpl(store);

        LeaderElection<String> le = cs.getLeaderElection(String.class, path, __ -> {
        });

        assertEquals(le.elect("test-1").join(), LeaderElectionState.Leading);
        assertEquals(le.getLeaderValue().join(), Optional.of("test-1"));

        // Closing the leader releases the leadership; reads on the closed instance must report an
        // empty leader without waiting (recovery paths key off the "no leader" condition).
        le.close();
        assertEquals(le.getLeaderValue().join(), Optional.empty());
        assertEquals(le.getLeaderValueIfPresent(), Optional.empty());
    }

    @Test(dataProvider = "impl", timeOut = 30000)
    public void electAfterCloseRunsANewElection(String provider, Supplier<String> urlSupplier) throws Exception {
        @Cleanup
        MetadataStoreExtended store = MetadataStoreExtended.create(urlSupplier.get(),
                MetadataStoreConfig.builder().fsyncEnable(false).build());

        String path = newKey();

        @Cleanup
        CoordinationService cs = new CoordinationServiceImpl(store);

        @Cleanup
        LeaderElection<String> le = cs.getLeaderElection(String.class, path, __ -> {
        });

        assertEquals(le.elect("test-1").join(), LeaderElectionState.Leading);
        le.close();

        // Re-electing on a closed instance reopens it (the broker's LeaderElectionService is
        // close()d and start()ed again to force a leadership change).
        assertEquals(le.elect("test-1").join(), LeaderElectionState.Leading);
        assertEquals(le.getLeaderValue().join(), Optional.of("test-1"));
        assertEquals(le.getLeaderValueIfPresent(), Optional.of("test-1"));
    }

    @Test(dataProvider = "impl", timeOut = 30000)
    public void observerReadsLeaderValueFromStore(String provider, Supplier<String> urlSupplier) throws Exception {
        @Cleanup
        MetadataStoreExtended store = MetadataStoreExtended.create(urlSupplier.get(),
                MetadataStoreConfig.builder().fsyncEnable(false).build());

        String path = newKey();

        @Cleanup
        CoordinationService cs = new CoordinationServiceImpl(store);
        @Cleanup
        CoordinationService observerCs = new CoordinationServiceImpl(store);

        @Cleanup
        LeaderElection<String> le = cs.getLeaderElection(String.class, path, __ -> {
        });
        // The observer never calls elect(): there is no local election cycle to wait for, so the
        // authoritative read goes directly to the metadata store, while the snapshot stays empty.
        @Cleanup
        LeaderElection<String> observer = observerCs.getLeaderElection(String.class, path, __ -> {
        });

        assertEquals(observer.getLeaderValue().join(), Optional.empty());

        assertEquals(le.elect("test-1").join(), LeaderElectionState.Leading);
        assertEquals(observer.getLeaderValue().join(), Optional.of("test-1"));
        assertEquals(observer.getLeaderValueIfPresent(), Optional.empty());
    }
}
