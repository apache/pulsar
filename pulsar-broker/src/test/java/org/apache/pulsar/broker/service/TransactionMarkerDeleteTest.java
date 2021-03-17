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

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import java.util.Collections;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.common.api.proto.CommandAck.AckType;
import org.apache.pulsar.common.api.proto.MarkersMessageIdData;
import org.apache.pulsar.common.protocol.Markers;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class TransactionMarkerDeleteTest extends BrokerTestBase{

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.baseSetup();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testTransactionMarkerDelete() throws Exception {
        ManagedLedger managedLedger = pulsar.getManagedLedgerFactory().open("test");
        PersistentTopic topic = mock(PersistentTopic.class);
        BrokerService brokerService = mock(BrokerService.class);
        PulsarService pulsarService = mock(PulsarService.class);
        ServiceConfiguration configuration = mock(ServiceConfiguration.class);
        doReturn(brokerService).when(topic).getBrokerService();
        doReturn(pulsarService).when(brokerService).getPulsar();
        doReturn(configuration).when(pulsarService).getConfig();
        doReturn(true).when(configuration).isTransactionCoordinatorEnabled();
        doReturn(managedLedger).when(topic).getManagedLedger();
        ManagedCursor cursor = managedLedger.openCursor("test");
        PersistentSubscription persistentSubscription = new PersistentSubscription(topic, "test",
                managedLedger.openCursor("test"), false);
        MarkersMessageIdData messageIdData = new MarkersMessageIdData()
                .setLedgerId(1)
                .setEntryId(1);
        Position position1 = managedLedger.addEntry("test".getBytes());
        managedLedger.addEntry(Markers
                .newTxnCommitMarker(1, 1, 1).array());
        Position position3 = managedLedger.addEntry(Markers
                .newTxnCommitMarker(1, 1, 1).array());
        assertEquals(cursor.getNumberOfEntriesInBacklog(true), 3);
        assertTrue(((PositionImpl) cursor.getMarkDeletedPosition()).compareTo((PositionImpl) position1) < 0);
        persistentSubscription.acknowledgeMessage(Collections.singletonList(position1),
                AckType.Individual, Collections.emptyMap());
        Thread.sleep(1000L);
        assertEquals(((PositionImpl) persistentSubscription.getCursor()
                .getMarkDeletedPosition()).compareTo((PositionImpl) position3), 0);
    }
}
