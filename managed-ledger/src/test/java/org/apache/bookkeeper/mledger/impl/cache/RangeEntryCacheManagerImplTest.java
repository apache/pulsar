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
package org.apache.bookkeeper.mledger.impl.cache;

import static org.apache.bookkeeper.mledger.util.ManagedLedgerTestUtil.defaultConfig;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.opentelemetry.api.OpenTelemetry;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryConfig;
import org.apache.bookkeeper.mledger.impl.EntryImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryMBeanImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class RangeEntryCacheManagerImplTest {
    private ManagedLedgerFactoryConfig config;
    private RangeEntryCacheManagerImpl manager;

    @BeforeMethod
    public void setup() {
        config = spy(new ManagedLedgerFactoryConfig());
        ManagedLedgerFactoryImpl factory = mock(ManagedLedgerFactoryImpl.class);
        when(factory.getConfig()).thenReturn(config);
        when(factory.getMbean()).thenReturn(mock(ManagedLedgerFactoryMBeanImpl.class));
        manager = new RangeEntryCacheManagerImpl(factory, mock(OrderedScheduler.class), OpenTelemetry.noop());
    }

    @AfterMethod(alwaysRun = true)
    public void cleanup() {
        if (manager != null) {
            manager.clear();
            manager.getInflightReadsLimiter().close();
        }
    }

    @DataProvider
    public Object[][] copyModes() {
        return new Object[][] {{false}, {true}};
    }

    @Test(dataProvider = "copyModes")
    public void testCopyModeUpdateDuringCacheCreation(boolean copyEntries) throws Exception {
        manager.updateCopyEntriesInCache(!copyEntries);
        EntryCache existing = manager.getEntryCache(managedLedger("existing"));
        ManagedLedgerImpl creating = managedLedger("creating");
        CountDownLatch constructionStarted = new CountDownLatch(1);
        CountDownLatch finishConstruction = new CountDownLatch(1);
        doAnswer(invocation -> {
            constructionStarted.countDown();
            assertThat(finishConstruction.await(10, TimeUnit.SECONDS)).isTrue();
            return null;
        }).when(creating).getLogger();
        ExecutorService executor = Executors.newSingleThreadExecutor(new DefaultThreadFactory("cache-creation"));
        try {
            Future<EntryCache> created = executor.submit(() -> manager.getEntryCache(creating));
            assertThat(constructionStarted.await(10, TimeUnit.SECONDS)).isTrue();
            manager.updateCopyEntriesInCache(copyEntries);
            assertCopyMode(existing, copyEntries);
            finishConstruction.countDown();
            assertCopyMode(created.get(10, TimeUnit.SECONDS), copyEntries);
        } finally {
            finishConstruction.countDown();
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test(dataProvider = "copyModes")
    public void testOlderUpdateCannotOverwriteCopyModeForExistingAndNewCaches(boolean copyEntries) throws Exception {
        EntryCache existing = manager.getEntryCache(managedLedger("existing"));
        CountDownLatch olderPolicyStored = new CountDownLatch(1);
        CountDownLatch finishOlderUpdate = new CountDownLatch(1);
        doAnswer(invocation -> {
            invocation.callRealMethod();
            olderPolicyStored.countDown();
            assertThat(finishOlderUpdate.await(10, TimeUnit.SECONDS)).isTrue();
            return null;
        }).when(config).setCopyEntriesInCache(!copyEntries);
        ExecutorService executor = Executors.newSingleThreadExecutor(new DefaultThreadFactory("cache-policy-update"));
        try {
            Future<?> olderUpdate = executor.submit(() -> manager.updateCopyEntriesInCache(!copyEntries));
            assertThat(olderPolicyStored.await(10, TimeUnit.SECONDS)).isTrue();
            // Publish another cache while the older update has not returned, then finish a newer policy update.
            EntryCache created = manager.getEntryCache(managedLedger("created-during-update"));
            manager.updateCopyEntriesInCache(copyEntries);
            finishOlderUpdate.countDown();
            olderUpdate.get(10, TimeUnit.SECONDS);
            assertThat(config.isCopyEntriesInCache()).isEqualTo(copyEntries);
            assertCopyMode(existing, copyEntries);
            assertCopyMode(created, copyEntries);
        } finally {
            finishOlderUpdate.countDown();
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    private static ManagedLedgerImpl managedLedger(String name) {
        ManagedLedgerImpl ledger = mock(ManagedLedgerImpl.class);
        when(ledger.getName()).thenReturn(name);
        when(ledger.getConfig()).thenReturn(defaultConfig());
        return ledger;
    }

    private static void assertCopyMode(EntryCache cache, boolean copyEntries) {
        ByteBuf source = Unpooled.directBuffer(128).writeInt(42);
        EntryImpl entry = EntryImpl.create(1, 0, source);
        source.release();
        try {
            assertThat(cache.insert(entry)).isTrue();
        } finally {
            entry.release();
        }
        assertThat(cache.getSize()).isEqualTo(Integer.BYTES);
        assertThat(source.refCnt()).as("source references after inserting into %s", cache.getName())
                .isEqualTo(copyEntries ? 0 : 1);
    }
}
