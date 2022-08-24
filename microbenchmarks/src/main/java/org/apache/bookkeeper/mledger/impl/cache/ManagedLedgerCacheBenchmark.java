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
package org.apache.bookkeeper.mledger.impl.cache;

import com.google.common.collect.ImmutableMap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.bookkeeper.client.PulsarMockBookKeeper;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryConfig;
import org.apache.bookkeeper.mledger.impl.EntryImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Threads(16)
@Fork(1)
@Warmup(iterations = 1, time = 10, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 3, time = 10, timeUnit = TimeUnit.SECONDS)
public class ManagedLedgerCacheBenchmark {

    private static Map<String, String> cacheManagers = ImmutableMap.of(
            "RangeEntryCacheManager", RangeEntryCacheManagerImpl.class.getName(),
            "SharedEntryCacheManager", SharedEntryCacheManagerImpl.class.getName());

    public enum CopyMode {
        Copy,
        RefCount,
    }

    @State(Scope.Benchmark)
    public static class TestState {
        @Param({
//                "RangeEntryCacheManager",
                "SharedEntryCacheManager",
        })
        private String entryCacheManagerName;

        @Param({
                "Copy",
                "RefCount",
        })
        private CopyMode copyMode;

        @Param({
//                "100",
//                "1024",
                "65536",
        })
        private int entrySize;

        private OrderedExecutor executor;
        private MetadataStoreExtended metadataStore;
        private ManagedLedgerFactoryImpl mlf;
        private EntryCache entryCache;

        private ByteBuf buffer;

        @Setup(Level.Trial)
        public void setup() throws Exception {
            executor = OrderedExecutor.newBuilder().build();
            metadataStore = MetadataStoreExtended.create("memory:local", MetadataStoreConfig.builder().build());

            ManagedLedgerFactoryConfig mlfc = new ManagedLedgerFactoryConfig();
            mlfc.setEntryCacheManagerClassName(cacheManagers.get(entryCacheManagerName));
            mlfc.setCopyEntriesInCache(copyMode == CopyMode.Copy);
            mlfc.setMaxCacheSize(1 * 1024 * 1024 * 1024);
            PulsarMockBookKeeper bkc = new PulsarMockBookKeeper(executor);
            mlf = new ManagedLedgerFactoryImpl(metadataStore, bkc, mlfc);

            ManagedLedgerImpl ml = (ManagedLedgerImpl) mlf.open("test-managed-ledger");

            entryCache = mlf.getEntryCacheManager().getEntryCache(ml);

            buffer = PooledByteBufAllocator.DEFAULT.directBuffer();
            buffer.writeBytes(new byte[entrySize]);
        }

        @TearDown(Level.Trial)
        public void tearDown() throws Exception {
            mlf.shutdown();
            metadataStore.close();

            System.out.println("REF-COUNT: " + buffer.refCnt());
            buffer.release();
            executor.shutdownNow();
        }
    }

    private static final AtomicLong ledgerIdSeq = new AtomicLong();

    @State(Scope.Thread)
    public static class ThreadState {
        private long ledgerId;
        private long entryId;

        @Setup(Level.Iteration)
        public void setup() throws Exception {
            ledgerId = ledgerIdSeq.incrementAndGet();
            entryId = 0;
        }
    }

    @Benchmark
    public void insertIntoCache(TestState s, ThreadState ts) {
        EntryImpl entry = EntryImpl.create(ts.ledgerId, ts.entryId, s.buffer.duplicate());
        s.entryCache.insert(entry);
        ts.entryId++;
        entry.release();
    }
}
