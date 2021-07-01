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

import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.client.api.BookKeeper;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.client.api.ListLedgersResult;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;
import org.testng.annotations.Test;

/**
 * With BookKeeper Opportunistic Striping feature we can allow Pulsar to work
 * with only WQ bookie during temporary outages of some bookie.
 */
@Test(groups = "broker")
public class OpportunisticStripingTest extends BkEnsemblesTestBase {

    public OpportunisticStripingTest() {
        // starting only two bookies
        super(2);
    }

    @Override
    protected void configurePulsar(ServiceConfiguration config) {
        // we would like to stripe over 5 bookies
        config.setManagedLedgerDefaultEnsembleSize(5);
        // we want 2 copies for each entry
        config.setManagedLedgerDefaultWriteQuorum(2);
        config.setManagedLedgerDefaultAckQuorum(2);

        config.setBrokerDeleteInactiveTopicsEnabled(false);
        config.getProperties().setProperty("bookkeeper_opportunisticStriping", "true");
    }

    @Test
    public void testOpportunisticStriping() throws Exception {

        try (PulsarClient client = PulsarClient.builder()
                .serviceUrl(pulsar.getWebServiceAddress())
                .statsInterval(0, TimeUnit.SECONDS)
                .build();) {

            final String ns1 = "prop/usc/opportunistic1";
            admin.namespaces().createNamespace(ns1);

            final String topic1 = "persistent://" + ns1 + "/my-topic";
            Producer<byte[]> producer = client.newProducer().topic(topic1).create();
            for (int i = 0; i < 10; i++) {
                String message = "my-message-" + i;
                producer.send(message.getBytes());
            }

            // verify that all ledgers has the proper writequorumsize,
            // equals to the number of available bookies (in this case 2)
            ClientConfiguration clientConfiguration = new ClientConfiguration();
            clientConfiguration.setZkServers("localhost:" + this.bkEnsemble.getZookeeperPort());

            try (BookKeeper bkAdmin = BookKeeper.newBuilder(clientConfiguration).build()) {
                try (ListLedgersResult list = bkAdmin.newListLedgersOp().execute().get();) {
                    int count = 0;
                    for (long ledgerId : list.toIterable()) {
                        LedgerMetadata ledgerMetadata = bkAdmin.getLedgerMetadata(ledgerId).get();
                        assertEquals(2, ledgerMetadata.getEnsembleSize());
                        assertEquals(2, ledgerMetadata.getWriteQuorumSize());
                        assertEquals(2, ledgerMetadata.getAckQuorumSize());
                        count++;
                    }
                    assertTrue(count > 0);
                }
            }
        }
    }

}
