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

import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandAck.AckType;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Collections;

public class TransactionMarkerDeleteTest extends BrokerTestBase{

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.baseSetup();
    }

    @AfterMethod
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void TransactionMarkerDeleteTest() throws Exception {
        ManagedLedger managedLedger = pulsar.getManagedLedgerFactory().open("test");
        Position position1 = managedLedger.addEntry("test".getBytes());
        managedLedger.addEntry("test".getBytes());
        Position position3 = managedLedger.addEntry("test".getBytes());
        PersistentTopic topic = mock(PersistentTopic.class);
        doReturn(managedLedger).when(topic).getManagedLedger();
        managedLedger.openCursor("test");
        PersistentSubscription persistentSubscription = new PersistentSubscription(topic, "test",
                managedLedger.openCursor("test"), false);
        persistentSubscription.acknowledgeMessage(Collections.singletonList(position1),
                AckType.Individual, Collections.emptyMap());
        assertEquals(0, ((PositionImpl) persistentSubscription.getCursor()
                .getMarkDeletedPosition()).compareTo((PositionImpl) position3));
    }
}
