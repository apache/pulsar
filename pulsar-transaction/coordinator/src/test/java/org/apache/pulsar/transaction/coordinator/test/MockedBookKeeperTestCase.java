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
package org.apache.pulsar.transaction.coordinator.test;

import java.lang.reflect.Method;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.CustomLog;
import org.apache.bookkeeper.client.PulsarMockBookKeeper;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryConfig;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.impl.FaultInjectionMetadataStore;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;

/**
 * A class runs several bookie servers for testing.
 */
@CustomLog
public abstract class MockedBookKeeperTestCase {
    protected FaultInjectionMetadataStore metadataStore;

    // BookKeeper related variables
    protected PulsarMockBookKeeper bkc;
    protected int numBookies;

    protected ManagedLedgerFactoryImpl factory;

    protected ClientConfiguration baseClientConf = new ClientConfiguration();

    protected OrderedScheduler executor;
    protected ExecutorService cachedExecutor;

    public MockedBookKeeperTestCase() {
        // By default start a 3 bookies cluster
        this(3);
    }

    public MockedBookKeeperTestCase(int numBookies) {
        this.numBookies = numBookies;
    }

    @BeforeMethod(alwaysRun = true)
    public void setUp(Method method) throws Exception {
        log.info().attr("value", method).log(">>>>>> starting");
        metadataStore = new FaultInjectionMetadataStore(MetadataStoreExtended.create("memory:local",
                MetadataStoreConfig.builder()
                        .metadataStoreName("metastore-" + method.getName())
                        .build()));
        try {
            // start bookkeeper service
            startBookKeeper();
        } catch (Exception e) {
            log.error().exception(e).log("Error setting up");
            throw e;
        }

        ManagedLedgerFactoryConfig conf = createManagedLedgerFactoryConfig();
        factory = new ManagedLedgerFactoryImpl(metadataStore, bkc, conf);
    }

    protected ManagedLedgerFactoryConfig createManagedLedgerFactoryConfig() {
        return new ManagedLedgerFactoryConfig();
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown(Method method) {
        try {
            log.info("@@@@@@@@@ stopping " + method);
            factory.shutdown();
            factory = null;
            stopBookKeeper();
            stopMetadataStore();
            metadataStore.close();
            log.info().attr("value", method).log("--------- stopped");
        } catch (Exception e) {
            log.error().exception(e).log("tearDown Error");
        }
    }

    @BeforeClass(alwaysRun = true)
    public void setUpClass() {
        executor = OrderedScheduler.newSchedulerBuilder().numThreads(2).name("test").build();
        cachedExecutor = Executors.newCachedThreadPool();
    }

    @AfterClass(alwaysRun = true)
    public void tearDownClass() {
        executor.shutdownNow();
        cachedExecutor.shutdownNow();
    }

    /**
     * Start cluster.
     *
     * @throws Exception
     */
    protected void startBookKeeper() throws Exception {
        for (int i = 0; i < numBookies; i++) {
            metadataStore.put("/ledgers/available/192.168.1.1:" + (5000 + i), "".getBytes(), Optional.empty());
        }

        metadataStore.put("/ledgers/LAYOUT", "1\nflat:1".getBytes(), Optional.empty());

        bkc = new PulsarMockBookKeeper(executor);
    }

    protected void stopBookKeeper() throws Exception {
        bkc.shutdown();
    }

    protected void stopMetadataStore() {
        metadataStore.setAlwaysFail(new MetadataStoreException("error"));
    }

}