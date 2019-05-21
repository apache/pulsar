package org.apache.bookkeeper.mledger.offload.filesystem.impl;


import com.google.common.util.concurrent.MoreExecutors;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.PulsarMockBookKeeper;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.mledger.LedgerOffloader;
import org.apache.bookkeeper.mledger.offload.filesystem.FileStoreTestBase;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.MockZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static java.nio.charset.StandardCharsets.UTF_8;

public class FileSystemManagedLedgerOffloaderTest extends FileStoreTestBase {
    private final PulsarMockBookKeeper bk;


    public FileSystemManagedLedgerOffloaderTest() throws Exception {
        this.bk = new PulsarMockBookKeeper(createMockZooKeeper(), scheduler.chooseThread(this));
    }

    private static MockZooKeeper createMockZooKeeper() throws Exception {
        MockZooKeeper zk = MockZooKeeper.newInstance(MoreExecutors.newDirectExecutorService());
        List<ACL> dummyAclList = new ArrayList<ACL>(0);

        ZkUtils.createFullPathOptimistic(zk, "/ledgers/available/192.168.1.1:" + 5000,
                "".getBytes(UTF_8), dummyAclList, CreateMode.PERSISTENT);

        zk.create("/ledgers/LAYOUT", "1\nflat:1".getBytes(UTF_8), dummyAclList,
                CreateMode.PERSISTENT);
        return zk;
    }

    private ReadHandle buildReadHandle() throws Exception {

        LedgerHandle lh = bk.createLedger(1,1,1, BookKeeper.DigestType.CRC32, "foobar".getBytes());

        int i = 0;
        int blocksWritten = 1;
        int entries = 0;

        while (blocksWritten < 601) {
            byte[] entry = ("foobar"+i).getBytes();
            int sizeInBlock = entry.length + 12 /* ENTRY_HEADER_SIZE */;
            blocksWritten++;
            entries++;

            lh.addEntry(entry);
            i++;
        }

        lh.close();

        return bk.newOpenLedgerOp().withLedgerId(lh.getId())
                .withPassword("foobar".getBytes()).withDigestType(DigestType.CRC32).execute().get();
    }
    @Test
    public void testOffloadAndRead() throws Exception {
        ReadHandle toWrite = buildReadHandle();
        LedgerOffloader offloader = fileSystemManagedLedgerOffloader;
        UUID uuid = UUID.randomUUID();
        Map<String, String> map = new HashMap<>();
        map.put("ManagedLedgerName", "public/default/test111");
        offloader.offload(toWrite, uuid, map).get();
        ReadHandle toTest = offloader.readOffloaded(toWrite.getId(), uuid, map).get();
        Assert.assertEquals(toTest.getLastAddConfirmed(), toWrite.getLastAddConfirmed());

        try (LedgerEntries toWriteEntries = toWrite.read(0, toWrite.getLastAddConfirmed());
             LedgerEntries toTestEntries = toTest.read(0, toTest.getLastAddConfirmed())) {
            Iterator<LedgerEntry> toWriteIter = toWriteEntries.iterator();
            Iterator<LedgerEntry> toTestIter = toTestEntries.iterator();

            while (toWriteIter.hasNext() && toTestIter.hasNext()) {
                LedgerEntry toWriteEntry = toWriteIter.next();
                LedgerEntry toTestEntry = toTestIter.next();

                Assert.assertEquals(toWriteEntry.getLedgerId(), toTestEntry.getLedgerId());
                Assert.assertEquals(toWriteEntry.getEntryId(), toTestEntry.getEntryId());
                Assert.assertEquals(toWriteEntry.getLength(), toTestEntry.getLength());
                Assert.assertEquals(toWriteEntry.getEntryBuffer(), toTestEntry.getEntryBuffer());
            }
            Assert.assertFalse(toWriteIter.hasNext());
            Assert.assertFalse(toTestIter.hasNext());
        }
    }

}
