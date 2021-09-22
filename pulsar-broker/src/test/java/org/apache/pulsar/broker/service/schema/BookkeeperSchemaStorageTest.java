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
package org.apache.pulsar.broker.service.schema;

import java.nio.ByteBuffer;
import org.apache.bookkeeper.client.api.BKException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.common.schema.LongSchemaVersion;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.zookeeper.ZooKeeper;
import org.testng.annotations.Test;

import static org.apache.pulsar.broker.service.schema.BookkeeperSchemaStorage.bkException;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

@Test(groups = "broker")
public class BookkeeperSchemaStorageTest {

    @Test
    public void testBkException() {
        Exception ex = bkException("test", BKException.Code.ReadException, 1, -1);
        assertEquals("Error while reading ledger -  ledger=1 - operation=test", ex.getMessage());
        ex = bkException("test", BKException.Code.ReadException, 1, 0);
        assertEquals("Error while reading ledger -  ledger=1 - operation=test - entry=0",
                ex.getMessage());
        ex = bkException("test", BKException.Code.QuorumException, 1, -1);
        assertEquals("Invalid quorum size on ensemble size -  ledger=1 - operation=test",
                ex.getMessage());
        ex = bkException("test", BKException.Code.QuorumException, 1, 0);
        assertEquals("Invalid quorum size on ensemble size -  ledger=1 - operation=test - entry=0",
                ex.getMessage());
    }

    @Test
    public void testVersionFromBytes() {
        long version = System.currentTimeMillis();

        ByteBuffer bbPre240 = ByteBuffer.allocate(Long.SIZE);
        bbPre240.putLong(version);
        byte[] versionBytesPre240 = bbPre240.array();

        ByteBuffer bbPost240 = ByteBuffer.allocate(Long.BYTES);
        bbPost240.putLong(version);
        byte[] versionBytesPost240 = bbPost240.array();

        PulsarService mockPulsarService = mock(PulsarService.class);
        when(mockPulsarService.getLocalMetadataStore()).thenReturn(mock(MetadataStoreExtended.class));
        BookkeeperSchemaStorage schemaStorage = new BookkeeperSchemaStorage(mockPulsarService, mock(ZooKeeper.class));
        assertEquals(new LongSchemaVersion(version), schemaStorage.versionFromBytes(versionBytesPre240));
        assertEquals(new LongSchemaVersion(version), schemaStorage.versionFromBytes(versionBytesPost240));
    }
}
