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
package org.apache.bookkeeper.mledger.offload.filesystem.impl;

import static org.testng.Assert.assertEquals;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.PulsarMockBookKeeper;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.mledger.LedgerOffloaderStats;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.OffloadPoliciesImpl;
import org.testng.annotations.Test;

public class FileSystemOffloaderLocalFileTest {
    private OrderedScheduler scheduler = OrderedScheduler.newSchedulerBuilder().numThreads(1).name("offloader").build();
    private LedgerOffloaderStats offloaderStats = LedgerOffloaderStats.create(true, true, scheduler, 60);


    private String getResourceFilePath(String name) {
        return getClass().getClassLoader().getResource(name).getPath();
    }

    @Test
    public void testReadWriteWithLocalFileUsingFileSystemURI() throws Exception {
        // prepare the offload policies
        final String basePath = "/tmp";
        OffloadPoliciesImpl offloadPolicies = new OffloadPoliciesImpl();
        offloadPolicies.setFileSystemURI("file://" + basePath);
        offloadPolicies.setManagedLedgerOffloadDriver("filesystem");
        offloadPolicies.setFileSystemProfilePath(getResourceFilePath("filesystem_offload_core_site.xml"));

        // initialize the offloader with the offload policies
        var offloader = FileSystemManagedLedgerOffloader.create(offloadPolicies, scheduler, offloaderStats);

        int numberOfEntries = 100;

        // prepare the data in bookkeeper
        BookKeeper bk = new PulsarMockBookKeeper(scheduler);
        LedgerHandle lh = bk.createLedger(1,1,1, BookKeeper.DigestType.CRC32, "".getBytes());
        for (int i = 0; i <  numberOfEntries; i++) {
            byte[] entry = ("foobar"+i).getBytes();
            lh.addEntry(entry);
        }
        lh.close();

        ReadHandle read = bk.newOpenLedgerOp()
            .withLedgerId(lh.getId())
            .withDigestType(DigestType.CRC32)
            .withPassword("".getBytes()).execute().get();

        final String mlName = TopicName.get("testWriteLocalFIle").getPersistenceNamingEncoding();
        Map<String, String> offloadDriverMetadata = new HashMap<>();
        offloadDriverMetadata.put("ManagedLedgerName", mlName);

        UUID uuid = UUID.randomUUID();
        offloader.offload(read, uuid, offloadDriverMetadata).get();
        ReadHandle toTest = offloader.readOffloaded(read.getId(), uuid, offloadDriverMetadata).get();
        assertEquals(toTest.getLastAddConfirmed(), read.getLastAddConfirmed());
        LedgerEntries toTestEntries = toTest.read(0, numberOfEntries - 1);
        LedgerEntries toWriteEntries = read.read(0,numberOfEntries - 1);
        Iterator<LedgerEntry> toTestIter = toTestEntries.iterator();
        Iterator<LedgerEntry> toWriteIter = toWriteEntries.iterator();
        while(toTestIter.hasNext()) {
            LedgerEntry toWriteEntry = toWriteIter.next();
            LedgerEntry toTestEntry = toTestIter.next();

            assertEquals(toWriteEntry.getLedgerId(), toTestEntry.getLedgerId());
            assertEquals(toWriteEntry.getEntryId(), toTestEntry.getEntryId());
            assertEquals(toWriteEntry.getLength(), toTestEntry.getLength());
            assertEquals(toWriteEntry.getEntryBuffer(), toTestEntry.getEntryBuffer());
        }
        toTestEntries = toTest.read(1, numberOfEntries - 1);
        toWriteEntries = read.read(1,numberOfEntries - 1);
        toTestIter = toTestEntries.iterator();
        toWriteIter = toWriteEntries.iterator();
        while(toTestIter.hasNext()) {
            LedgerEntry toWriteEntry = toWriteIter.next();
            LedgerEntry toTestEntry = toTestIter.next();

            assertEquals(toWriteEntry.getLedgerId(), toTestEntry.getLedgerId());
            assertEquals(toWriteEntry.getEntryId(), toTestEntry.getEntryId());
            assertEquals(toWriteEntry.getLength(), toTestEntry.getLength());
            assertEquals(toWriteEntry.getEntryBuffer(), toTestEntry.getEntryBuffer());
        }

        // check the file located in the local file system
        Path offloadedFilePath = Paths.get(basePath, mlName);
        assertEquals(Files.exists(offloadedFilePath), true);
    }
}
