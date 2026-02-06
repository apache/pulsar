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
package org.apache.pulsar.broker.service;

import static org.testng.Assert.assertEquals;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker")
public class ManagedLedgerConfigTest extends ProducerConsumerBase {

    @BeforeClass(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @DataProvider(name = "booleans")
    public Object[][] booleans() {
        return new Object[][] {
                {true},
                {false},
        };
    }

    @Test(dataProvider = "booleans")
    public void testConfigPersistIndividualAckAsLongArray(boolean enabled) throws Exception {
        pulsar.getConfiguration().setManagedLedgerPersistIndividualAckAsLongArray(enabled);
        final String tpName = BrokerTestUtil.newUniqueName("persistent://public/default/tp");
        admin.topics().createNonPartitionedTopic(tpName);
        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getTopic(tpName, true).get().get();
        ManagedLedgerConfig mlConf = topic.getManagedLedger().getConfig();
        assertEquals(mlConf.isPersistIndividualAckAsLongArray(), enabled);

        // cleanup.
        admin.topics().delete(tpName);
    }
}

