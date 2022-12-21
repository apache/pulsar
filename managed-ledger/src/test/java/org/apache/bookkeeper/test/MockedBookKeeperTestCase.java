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
package org.apache.bookkeeper.test;

import java.lang.reflect.Method;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.bookkeeper.client.PulsarMockBookKeeper;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryConfig;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.impl.FaultInjectionMetadataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;

/**
 * A class runs several bookie servers for testing.
 */
public abstract class MockedBookKeeperTestCase {

    static final Logger LOG = LoggerFactory.getLogger(MockedBookKeeperTestCase.class);

    // BookKeeper related variables
    protected PulsarMockBookKeeper bkc;
    protected int numBookies;

    protected ManagedLedgerFactoryImpl factory;

    protected OrderedScheduler executor;
    protected ExecutorService cachedExecutor;

    protected FaultInjectionMetadataStore metadataStore;

    public MockedBookKeeperTestCase() {
        // By default start a 3 bookies cluster
        this(3);
    }

    public MockedBookKeeperTestCase(int numBookies) {
        this.numBookies = numBookies;
    }

    @BeforeMethod(alwaysRun = true)
    public final void setUp(Method method) throws Exception {
        LOG.info(">>>>>> starting {}", method);
        metadataStore = new FaultInjectionMetadataStore(
                MetadataStoreExtended.create("memory:local", MetadataStoreConfig.builder().build()));

        try {
            // start bookkeeper service
            startBookKeeper();
        } catch (Exception e) {
            LOG.error("Error setting up", e);
            throw e;
        }

        ManagedLedgerFactoryConfig managedLedgerFactoryConfig = new ManagedLedgerFactoryConfig();
        // increase default cache eviction interval so that caching could be tested with less flakyness
        managedLedgerFactoryConfig.setCacheEvictionIntervalMs(200);
        factory = new ManagedLedgerFactoryImpl(metadataStore, bkc);

        setUpTestCase();
    }

    protected void setUpTestCase() throws Exception {

    }

    @AfterMethod(alwaysRun = true)
    public final void tearDown(Method method) {
        try {
            cleanUpTestCase();
        } catch (Exception e) {
            LOG.error("tearDown Error", e);
        }
        try {
            LOG.info("@@@@@@@@@ stopping " + method);
            factory.shutdown();
            factory = null;
            stopBookKeeper();
            LOG.info("--------- stopped {}", method);
        } catch (Exception e) {
            LOG.error("tearDown Error", e);
        }
    }

    protected void cleanUpTestCase() throws Exception {

    }

    @BeforeClass(alwaysRun = true)
    public final void setUpClass() {
        executor = OrderedScheduler.newSchedulerBuilder().numThreads(2).name("test").build();
        cachedExecutor = Executors.newCachedThreadPool();
    }

    @AfterClass(alwaysRun = true)
    public final void tearDownClass() {
        if (executor != null) {
            executor.shutdownNow();
        }
        if (cachedExecutor != null) {
            cachedExecutor.shutdownNow();
        }
    }

    /**
     * Start cluster
     *
     * @throws Exception
     */
    protected void startBookKeeper() throws Exception {
        for (int i = 0; i < numBookies; i++) {
            metadataStore.put( "/ledgers/available/192.168.1.1:" + (5000 + i), new byte[0], Optional.empty()).join();
        }

        metadataStore.put("/ledgers/LAYOUT", "1\nflat:1".getBytes(), Optional.empty()).join();

        bkc = new PulsarMockBookKeeper(executor);
    }

    protected void stopBookKeeper() {
        bkc.shutdown();
    }

    protected void stopMetadataStore() {
        metadataStore.setAlwaysFail(new MetadataStoreException("failed"));
    }
}
