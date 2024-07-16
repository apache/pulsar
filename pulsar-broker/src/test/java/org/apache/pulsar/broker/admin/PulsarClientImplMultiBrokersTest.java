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
package org.apache.pulsar.broker.admin;

import static org.testng.Assert.fail;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.MultiBrokerBaseTest;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.client.impl.LookupService;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.awaitility.reflect.WhiteboxImpl;
import org.testng.annotations.Test;

/**
 * Test multi-broker admin api.
 */
@Slf4j
@Test(groups = "broker-admin")
public class PulsarClientImplMultiBrokersTest extends MultiBrokerBaseTest {
    @Override
    protected int numberOfAdditionalBrokers() {
        return 3;
    }

    @Override
    protected void doInitConf() throws Exception {
        super.doInitConf();
        this.conf.setManagedLedgerMaxEntriesPerLedger(10);
    }

    @Override
    protected void onCleanup() {
        super.onCleanup();
    }

    @Test(timeOut = 30 * 1000)
    public void testReleaseUrlLookupServices() throws Exception {
        PulsarClientImpl pulsarClient = (PulsarClientImpl) additionalBrokerClients.get(0);
        Map<String, LookupService> urlLookupMap = WhiteboxImpl.getInternalState(pulsarClient, "urlLookupMap");
        assertEquals(urlLookupMap.size(), 0);
        for (PulsarService pulsar : additionalBrokers) {
            pulsarClient.getLookup(pulsar.getBrokerServiceUrl());
            pulsarClient.getLookup(pulsar.getWebServiceAddress());
        }
        assertEquals(urlLookupMap.size(), additionalBrokers.size() * 2);
        // Verify: lookup services will be release.
        pulsarClient.close();
        assertEquals(urlLookupMap.size(), 0);
        try {
            for (PulsarService pulsar : additionalBrokers) {
                pulsarClient.getLookup(pulsar.getBrokerServiceUrl());
                pulsarClient.getLookup(pulsar.getWebServiceAddress());
            }
            fail("Expected a error when calling pulsarClient.getLookup if getLookup was closed");
        } catch (IllegalStateException illegalArgumentException) {
            assertTrue(illegalArgumentException.getMessage().contains("has been closed"));
        }
        assertEquals(urlLookupMap.size(), 0);
    }
}
