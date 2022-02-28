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
package org.apache.pulsar.broker.stats.prometheus;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.discover.RegistrationClient;
import org.apache.bookkeeper.meta.MetadataClientDriver;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.NullStatsProvider;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stats.StatsProvider;
import org.apache.bookkeeper.stats.annotations.StatsDoc;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.resources.PulsarResources;
import org.apache.pulsar.common.policies.data.BookiesClusterInfo;
import org.apache.pulsar.common.policies.data.BookiesRackConfiguration;
import org.apache.pulsar.common.policies.data.RawBookieInfo;

public class BookkeeperIsolationGroupsStats {
    private static final String CURRENT_RACKS_COUNT = "current-racks-count";
    private static final String RACKS_COUNT = "racks-count";

    @StatsDoc(
            name = CURRENT_RACKS_COUNT,
            help = "Number of current racks"
    )
    private Gauge<Integer> currRackCountGauge;

    @StatsDoc(
            name = RACKS_COUNT,
            help = "Number of all racks"
    )
    private Gauge<Integer> allRackCountGauge;

    private StatsProvider statsProvider = new NullStatsProvider();
    private static final long UPDATE_INTERVAL_MILLIS = 60 * 1000;
    private long lastUpdateTime = -1;
    Map<String /*rackGroup*/, Integer /*rackCount*/> racksCount = Maps.newConcurrentMap();

    public BookkeeperIsolationGroupsStats(PulsarResources pulsarResources, BookKeeper bkClient, ServiceConfiguration conf,
                                          StatsProvider statsProvider) {
        if (!conf.isBookkeeperClientExposeStatsToPrometheus()) {
            return;
        }
        StatsLogger statsLogger = statsProvider.getStatsLogger("pulsar_managedRack_client");
        // current rack group's rack metrics
        Supplier<Integer> currRackCountSupplier = () -> {
            try {
                Integer currentGroupRackCount = getWritableBookieRackCount(pulsarResources, bkClient).
                        get(conf.getBookkeeperClientIsolationGroups());
                if (currentGroupRackCount == null) {
                    return -1;
                }
                return currentGroupRackCount.intValue();
            } catch (Exception e) {
                return -1;
            }
        };
        currRackCountGauge = new Gauge<Integer>() {
            @Override
            public Integer getDefaultValue() {
                return 0;
            }

            @Override
            public Integer getSample() {
                return currRackCountSupplier.get();
            }
        };
        statsLogger.registerGauge(CURRENT_RACKS_COUNT, currRackCountGauge);

        // all rack group's writable rack metrics
        Supplier<Map<String, Integer>> allRackCountSupplier = () -> {
            try {
                return getWritableBookieRackCount(pulsarResources, bkClient);
            } catch (Exception e) {
                return Maps.newConcurrentMap();
            }
        };
        allRackCountSupplier.get().forEach((group, rackCount) -> {
            allRackCountGauge = new Gauge<Integer>() {
                @Override
                public Integer getDefaultValue() {
                    return 0;
                }

                @Override
                public Integer getSample() {
                    return allRackCountSupplier.get().get(group);
                }
            };
            statsLogger.registerGauge(group + "-" + RACKS_COUNT, allRackCountGauge);
        });
    }

    public synchronized Map<String /*rackGroup*/, Integer /*rackCount*/> getWritableBookieRackCount(
            PulsarResources pulsarResources, BookKeeper bkClient) throws Exception {
        if (System.currentTimeMillis() - lastUpdateTime < UPDATE_INTERVAL_MILLIS) {
            return this.racksCount;
        }

        BookiesRackConfiguration rackInfos = getBookiesRackInfo(pulsarResources);
        BookiesClusterInfo writableBookies = getWritableBookies(bkClient);
        if (rackInfos == null || writableBookies == null) {
            return this.racksCount;
        }
        Map<String /*rackGroup*/, Set<String> /*rackSet*/> racksSet = Maps.newConcurrentMap();
        rackInfos.forEach((group, bookieInfoMap) -> {
            bookieInfoMap.forEach((address, bookieInfo) -> {
                if (writableBookies.getBookies().contains(new RawBookieInfo(address))) {
                    Set<String> rackSet = racksSet.computeIfAbsent(group, k -> Sets.newConcurrentHashSet());
                    rackSet.add(bookieInfo.getRack());
                }
            });
        });

        Map<String /*rackGroup*/, Integer /*rackCount*/> racksCountNew = Maps.newConcurrentMap();
        racksSet.forEach((group, rackSet) -> racksCountNew.put(group, rackSet.size()));

        lastUpdateTime = System.currentTimeMillis();
        this.racksCount = racksCountNew;
        return this.racksCount;
    }

    public BookiesRackConfiguration getBookiesRackInfo(PulsarResources pulsarResources) throws Exception {
        AtomicReference<BookiesRackConfiguration> bookiesRackCof =
                new AtomicReference<>(new BookiesRackConfiguration());
        pulsarResources.getBookieResources().get()
                .thenAccept(b -> {
                    bookiesRackCof.set(b.get());
                });
        return bookiesRackCof.get();
    }

    public BookiesClusterInfo getWritableBookies(BookKeeper bkClient) throws Exception {
        MetadataClientDriver metadataClientDriver = bkClient.getMetadataClientDriver();
        RegistrationClient registrationClient = metadataClientDriver.getRegistrationClient();

        Set<BookieId> writableBookies = registrationClient.getWritableBookies().get().getValue();
        List<RawBookieInfo> result = Lists.newArrayListWithExpectedSize(writableBookies.size());
        for (BookieId bookieId : writableBookies) {
            RawBookieInfo bookieInfo = new RawBookieInfo(bookieId.toString());
            result.add(bookieInfo);
        }
        return BookiesClusterInfo.builder().bookies(result).build();
    }
}
