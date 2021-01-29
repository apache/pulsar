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

package org.apache.bookkeeper.mledger.offload.jcloud;

import static org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl.isStreamingOffloadCompleted;
import static org.testng.Assert.assertEquals;
import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.LedgerOffloader;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.bookkeeper.mledger.offload.jcloud.impl.BlobStoreManagedLedgerOffloaderStreamingTest;
import org.apache.bookkeeper.mledger.offload.jcloud.provider.TieredStorageConfiguration;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.apache.pulsar.common.policies.data.OffloadPolicies;
import org.testng.Assert;
import org.testng.annotations.Test;

@Slf4j
public class StreamingOffloadTest extends MockedBookKeeperTestCase {

    BlobStoreManagedLedgerOffloaderStreamingTest offloaderGenerator = new BlobStoreManagedLedgerOffloaderStreamingTest();

    public StreamingOffloadTest() throws Exception {
    }

    @Test
    public void testStreamingOffload() throws ManagedLedgerException, InterruptedException, IOException,
            ExecutionException {
        LedgerOffloader offloader = offloaderGenerator.getOffloader(
                new HashMap<String, String>() {{
                    put(TieredStorageConfiguration.MAX_OFFLOAD_SEGMENT_SIZE_IN_BYTES, "1000");
                    put(offloaderGenerator.getConfig().getKeys(TieredStorageConfiguration.METADATA_FIELD_MAX_BLOCK_SIZE)
                            .get(0), "5242880");
                    put(TieredStorageConfiguration.MAX_OFFLOAD_SEGMENT_ROLLOVER_TIME_SEC, "600");
                    put("offloadMethod", OffloadPolicies.OffloadMethod.STREAMING_BASED.getStrValue());
                }}
        );
        System.out.println("offloader.getClass() = " + offloader.getClass());
        int ENTRIES_PER_LEDGER = 10;
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setMaxEntriesPerLedger(ENTRIES_PER_LEDGER);
        config.setMinimumRolloverTime(0, TimeUnit.SECONDS);
        config.setRetentionTime(10, TimeUnit.MINUTES);
        config.setRetentionSizeInMB(10);
        config.setLedgerOffloader(offloader);

        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("my_test_ledger", config);

        for (int i = 0; i < ENTRIES_PER_LEDGER * 2.5; i++) {
            String content = "entry-" + i;
            ledger.addEntry(content.getBytes());
        }
        final LedgerOffloader.OffloadHandle currentOffloadHandle = ledger.getCurrentOffloadHandle();
        currentOffloadHandle.close();
        final LedgerOffloader.OffloadResult offloadResult = currentOffloadHandle.getOffloadResultAsync().get();
        log.info("offloadResult = " + offloadResult);
        log.info("offload method: " + ledger.getOffloadMethod());
    }

    @Test
    public void testStreamingOffloadAndRead() throws ManagedLedgerException, InterruptedException, IOException,
            ExecutionException {
        LedgerOffloader offloader = offloaderGenerator.getOffloader(
                new HashMap<String, String>() {{
                    put(TieredStorageConfiguration.MAX_OFFLOAD_SEGMENT_SIZE_IN_BYTES, "1000");
                    put(offloaderGenerator.getConfig().getKeys(TieredStorageConfiguration.METADATA_FIELD_MAX_BLOCK_SIZE)
                            .get(0), "5242880");
                    put(TieredStorageConfiguration.MAX_OFFLOAD_SEGMENT_ROLLOVER_TIME_SEC, "600");
                    put("offloadMethod", OffloadPolicies.OffloadMethod.STREAMING_BASED.getStrValue());
                }}
        );
        int ENTRIES_PER_LEDGER = 10;
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setMaxEntriesPerLedger(ENTRIES_PER_LEDGER);
        config.setMinimumRolloverTime(0, TimeUnit.SECONDS);
        config.setRetentionTime(10, TimeUnit.MINUTES);
        config.setRetentionSizeInMB(10);
        config.setLedgerOffloader(offloader);

        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("test_read_and_write", config);

        for (int i = 0; i < ENTRIES_PER_LEDGER * 2.5; i++) {
            String content = "entry-" + i;
            ledger.addEntry(content.getBytes());
        }
        final LedgerOffloader.OffloadHandle currentOffloadHandle = ledger.getCurrentOffloadHandle();
        currentOffloadHandle.close();
        final LedgerOffloader.OffloadResult offloadResult = currentOffloadHandle.getOffloadResultAsync().get();
        log.info("offloadResult = " + offloadResult);
        log.info("offload method: " + ledger.getOffloadMethod());
        while (!isStreamingOffloadCompleted(ledger.getLedgersInfoAsList().get(0))) {
            Thread.sleep(10);
        }
        Assert.assertTrue(isStreamingOffloadCompleted(ledger.getLedgersInfoAsList().get(0)));
        Assert.assertTrue(isStreamingOffloadCompleted(ledger.getLedgersInfoAsList().get(1)));

        ManagedCursor cursor = ledger.newNonDurableCursor(PositionImpl.earliest);
        int i = 0;
        for (Entry e : cursor.readEntries(10)) {
            assertEquals(new String(e.getData()), "entry-" + i++);
        }

        for (Entry e : cursor.readEntries(10)) {
            assertEquals(new String(e.getData()), "entry-" + i++);
        }

        for (Entry e : cursor.readEntries(5)) {
            assertEquals(new String(e.getData()), "entry-" + i++);
        }
    }

    @Test
    public void testSetStreamingOffloadAfterCreated() throws ManagedLedgerException, InterruptedException, IOException,
            ExecutionException {
        LedgerOffloader offloader = offloaderGenerator.getOffloader(
                new HashMap<String, String>() {{
                    put(TieredStorageConfiguration.MAX_OFFLOAD_SEGMENT_SIZE_IN_BYTES, "1000");
                    put(offloaderGenerator.getConfig().getKeys(TieredStorageConfiguration.METADATA_FIELD_MAX_BLOCK_SIZE)
                            .get(0), "5242880");
                    put(TieredStorageConfiguration.MAX_OFFLOAD_SEGMENT_ROLLOVER_TIME_SEC, "600");
                    put("offloadMethod", OffloadPolicies.OffloadMethod.LEDGER_BASED.getStrValue());
                }}
        );
        int ENTRIES_PER_LEDGER = 10;
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setMaxEntriesPerLedger(ENTRIES_PER_LEDGER);
        config.setMinimumRolloverTime(0, TimeUnit.SECONDS);
        config.setRetentionTime(10, TimeUnit.MINUTES);
        config.setRetentionSizeInMB(10);
        config.setLedgerOffloader(offloader);

        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("test_read_and_write", config);
        int beforeSetStreaming = 1;
        for (int i = 0; i < beforeSetStreaming; i++) {
            String content = "entry-" + i;
            ledger.addEntry(content.getBytes());
        }

        LedgerOffloader streamingOffloader = offloaderGenerator.getOffloader(
                new HashMap<String, String>() {{
                    put(TieredStorageConfiguration.MAX_OFFLOAD_SEGMENT_SIZE_IN_BYTES, "1000");
                    put(offloaderGenerator.getConfig().getKeys(TieredStorageConfiguration.METADATA_FIELD_MAX_BLOCK_SIZE)
                            .get(0), "5242880");
                    put(TieredStorageConfiguration.MAX_OFFLOAD_SEGMENT_ROLLOVER_TIME_SEC, "600");
                    put("offloadMethod", OffloadPolicies.OffloadMethod.STREAMING_BASED.getStrValue());
                }}
        );

        config.setLedgerOffloader(streamingOffloader);
        ledger.setConfig(config);

        for (int i = beforeSetStreaming; i < ENTRIES_PER_LEDGER * 2.5; i++) {
            String content = "entry-" + i;
            ledger.addEntry(content.getBytes());
        }
        Thread.sleep(5000);
        final LedgerOffloader.OffloadHandle currentOffloadHandle = ledger.getCurrentOffloadHandle();
        currentOffloadHandle.close();
        final LedgerOffloader.OffloadResult offloadResult = currentOffloadHandle.getOffloadResultAsync().get();
        log.info("offloadResult = " + offloadResult);
        log.info("offload method: " + ledger.getOffloadMethod());
        while (!isStreamingOffloadCompleted(ledger.getLedgersInfoAsList().get(0))) {
            Thread.sleep(10);
        }
        Assert.assertTrue(isStreamingOffloadCompleted(ledger.getLedgersInfoAsList().get(0)));
        Assert.assertTrue(isStreamingOffloadCompleted(ledger.getLedgersInfoAsList().get(1)));

        ManagedCursor cursor = ledger.newNonDurableCursor(PositionImpl.earliest);
        int i = 0;
        for (Entry e : cursor.readEntries(10)) {
            assertEquals(new String(e.getData()), "entry-" + i++);
        }

        for (Entry e : cursor.readEntries(10)) {
            assertEquals(new String(e.getData()), "entry-" + i++);
        }

        for (Entry e : cursor.readEntries(5)) {
            assertEquals(new String(e.getData()), "entry-" + i++);
        }
    }
}
