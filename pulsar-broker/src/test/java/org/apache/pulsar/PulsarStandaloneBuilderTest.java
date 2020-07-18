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
package org.apache.pulsar;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class PulsarStandaloneBuilderTest {
    @Test
    public void testBuildCreatesConfigObjects() throws FileNotFoundException {
        File testConfigFile = new File("tmp." + System.currentTimeMillis() + ".properties");
        if (testConfigFile.exists()) {
            testConfigFile.delete();
        }
        PrintWriter printWriter = new PrintWriter(testConfigFile);
        printWriter.println("managedLedgerDefaultEnsembleSize=1");
        printWriter.println("managedLedgerDefaultWriteQuorum=1");
        printWriter.println("managedLedgerDefaultAckQuorum=1");
        printWriter.println("journalMaxSizeMB=1024");
        printWriter.println("journalPreAllocSizeMB=8");
        printWriter.println("journalWriteBufferSizeKB=32");
        printWriter.close();
        testConfigFile.deleteOnExit();

        final PulsarStandalone pulsarStandalone = PulsarStandaloneBuilder.instance()
                .withConfigFile(testConfigFile.getAbsolutePath())
                .build();

        assertNotNull(pulsarStandalone.getConfig(), "ServiceConfiguration must not be null");
        assertNotNull(pulsarStandalone.getBkServerConfig(), "ServerConfiguration must not be null");
        assertEquals(pulsarStandalone.getConfig().getManagedLedgerDefaultEnsembleSize(), 1);
        assertEquals(pulsarStandalone.getConfig().getManagedLedgerDefaultWriteQuorum(), 1);
        assertEquals(pulsarStandalone.getConfig().getManagedLedgerDefaultAckQuorum(), 1);
        assertEquals(pulsarStandalone.getBkServerConfig().getMaxJournalSizeMB(), 1024);
        assertEquals(pulsarStandalone.getBkServerConfig().getJournalPreAllocSizeMB(), 8);
        assertEquals(pulsarStandalone.getBkServerConfig().getJournalWriteBufferSizeKB(), 32);
    }
}
