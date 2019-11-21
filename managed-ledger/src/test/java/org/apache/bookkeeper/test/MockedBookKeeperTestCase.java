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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.bookkeeper.client.PulsarMockBookKeeper;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryConfig;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.MockZooKeeper;
import org.apache.zookeeper.ZooDefs;
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

    // ZooKeeper related variables
    protected MockZooKeeper zkc;

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

    @BeforeMethod
    public void setUp(Method method) throws Exception {
        LOG.info(">>>>>> starting {}", method);
        try {
            // start bookkeeper service
            startBookKeeper();
        } catch (Exception e) {
            LOG.error("Error setting up", e);
            throw e;
        }

        ManagedLedgerFactoryConfig conf = new ManagedLedgerFactoryConfig();
        factory = new ManagedLedgerFactoryImpl(bkc, zkc, conf);

        zkc.create("/managed-ledgers", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown(Method method) {
        try {
            LOG.info("@@@@@@@@@ stopping " + method);
            factory.shutdown();
            factory = null;
            stopBookKeeper();
            stopZooKeeper();
            LOG.info("--------- stopped {}", method);
        } catch (Exception e) {
            LOG.error("tearDown Error", e);
        }
    }

    @BeforeClass
    public void setUpClass() {
        executor = OrderedScheduler.newSchedulerBuilder().numThreads(2).name("test").build();
        cachedExecutor = Executors.newCachedThreadPool();
    }

    @AfterClass
    public void tearDownClass() {
        executor.shutdown();
        cachedExecutor.shutdown();
    }

    /**
     * Start cluster
     *
     * @throws Exception
     */
    protected void startBookKeeper() throws Exception {
        zkc = MockZooKeeper.newInstance();
        for (int i = 0; i < numBookies; i++) {
            ZkUtils.createFullPathOptimistic(zkc, "/ledgers/available/192.168.1.1:" + (5000 + i), "".getBytes(), null,
                    null);
        }

        zkc.create("/ledgers/LAYOUT", "1\nflat:1".getBytes(), null, null);

        bkc = new PulsarMockBookKeeper(zkc, executor.chooseThread(this));
    }

    protected void stopBookKeeper() throws Exception {
        bkc.shutdown();
    }

    protected void stopZooKeeper() throws Exception {
        zkc.shutdown();
    }

}
