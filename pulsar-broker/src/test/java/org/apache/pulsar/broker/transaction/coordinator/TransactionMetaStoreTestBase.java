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
package org.apache.pulsar.broker.transaction.coordinator;

import java.util.Optional;
import org.apache.pulsar.PulsarTransactionCoordinatorMetadataSetup;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.transaction.TransactionCoordinatorClient;
import org.apache.pulsar.client.impl.transaction.TransactionCoordinatorClientImpl;
import org.apache.pulsar.tests.TestRetrySupport;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

public abstract class TransactionMetaStoreTestBase extends TestRetrySupport {

    private static final Logger log = LoggerFactory.getLogger(TransactionMetaStoreTestBase.class);

    LocalBookkeeperEnsemble bkEnsemble;
    protected PulsarAdmin[] pulsarAdmins = new PulsarAdmin[BROKER_COUNT];
    protected PulsarClient pulsarClient;
    protected static int BROKER_COUNT = 5;
    protected ServiceConfiguration[] configurations = new ServiceConfiguration[BROKER_COUNT];
    protected PulsarService[] pulsarServices = new PulsarService[BROKER_COUNT];

    protected TransactionCoordinatorClient transactionCoordinatorClient;

    @BeforeClass(alwaysRun = true)
    protected final void setup() throws Exception {
        log.info("---- Initializing {} -----", getClass().getSimpleName());
        // Start local bookkeeper ensemble
        bkEnsemble = new LocalBookkeeperEnsemble(3, 0, () -> 0);
        bkEnsemble.start();

        String[] args = new String[]{
            "--cluster", "my-cluster",
            "--configuration-store", "localhost:" + bkEnsemble.getZookeeperPort(),
            "--initial-num-transaction-coordinators", "16"};

        PulsarTransactionCoordinatorMetadataSetup.main(args);

        // start brokers
        for (int i = 0; i < BROKER_COUNT; i++) {
            ServiceConfiguration config = new ServiceConfiguration();
            config.setBrokerShutdownTimeoutMs(0L);
            config.setBrokerServicePort(Optional.of(0));
            config.setClusterName("my-cluster");
            config.setAdvertisedAddress("localhost");
            config.setWebServicePort(Optional.of(0));
            config.setZookeeperServers("127.0.0.1" + ":" + bkEnsemble.getZookeeperPort());
            config.setDefaultNumberOfNamespaceBundles(1);
            config.setLoadBalancerEnabled(false);
            config.setAcknowledgmentAtBatchIndexLevelEnabled(true);
            config.setTransactionCoordinatorEnabled(true);
            configurations[i] = config;

            pulsarServices[i] = Mockito.spy(new PulsarService(config));
            pulsarServices[i].start();

            pulsarAdmins[i] = PulsarAdmin.builder()
                    .serviceHttpUrl(pulsarServices[i].getWebServiceAddress())
                    .build();
        }

        Thread.sleep(100);

        afterPulsarStart();

        if (pulsarClient != null) {
            pulsarClient.shutdown();
        }
        pulsarClient = PulsarClient.builder().
            serviceUrl(pulsarServices[0].getBrokerServiceUrl())
            .build();
        transactionCoordinatorClient = new TransactionCoordinatorClientImpl(pulsarClient);
        transactionCoordinatorClient.start();

        Thread.sleep(3000);

        afterSetup();
    }

    protected void afterSetup() throws Exception {
        // template methods to override in subclasses
    }


    protected void afterPulsarStart() throws Exception {
        // template methods to override in subclasses
    }

    @AfterClass(alwaysRun = true)
    public final void shutdownAll() throws Exception {
        cleanup();
    }

    @Override
    protected void cleanup() throws Exception {
        for (PulsarAdmin admin : pulsarAdmins) {
            if (admin != null) {
                admin.close();
            }
        }
        if (pulsarClient != null) {
            pulsarClient.close();
        }
        for (PulsarService service : pulsarServices) {
            if (service != null) {
                service.close();
            }
        }
        Mockito.reset();
    }
}
