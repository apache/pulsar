/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.test;

import java.lang.reflect.Method;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.bookkeeper.client.MockBookKeeper;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryConfig;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.bookkeeper.mledger.impl.MetaStoreImplZookeeper.ZNodeProtobufFormat;
import org.apache.bookkeeper.util.OrderedSafeExecutor;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.zookeeper.MockZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;

/**
 * A class runs several bookie servers for testing.
 */
public abstract class MockedBookKeeperTestCase {

    static final Logger LOG = LoggerFactory.getLogger(MockedBookKeeperTestCase.class);

    // ZooKeeper related variables
    protected MockZooKeeper zkc;

    // BookKeeper related variables
    protected MockBookKeeper bkc;
    protected int numBookies;

    protected ManagedLedgerFactoryImpl factory;

    protected ClientConfiguration baseClientConf = new ClientConfiguration();

    protected OrderedSafeExecutor executor;
    protected ExecutorService cachedExecutor;
    
    protected ZNodeProtobufFormat protobufFormat = ZNodeProtobufFormat.Text;

    public MockedBookKeeperTestCase() {
        // By default start a 3 bookies cluster
        this(3);
    }

    public MockedBookKeeperTestCase(int numBookies) {
        this.numBookies = numBookies;
    }
    
    @DataProvider(name = "protobufFormat")
    public static Object[][] protobufFormat() {
        return new Object[][] { { ZNodeProtobufFormat.Text }, { ZNodeProtobufFormat.Binary } };
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

        executor = new OrderedSafeExecutor(2, "test");
        cachedExecutor = Executors.newCachedThreadPool();
        ManagedLedgerFactoryConfig conf = new ManagedLedgerFactoryConfig();
        conf.setUseProtobufBinaryFormatInZK(protobufFormat == ZNodeProtobufFormat.Binary);
        factory = new ManagedLedgerFactoryImpl(bkc, zkc, conf);
    }

    @AfterMethod
    public void tearDown(Method method) throws Exception {
        LOG.info("@@@@@@@@@ stopping " + method);
        factory.shutdown();
        factory = null;
        stopBookKeeper();
        stopZooKeeper();
        executor.shutdown();
        cachedExecutor.shutdown();
        LOG.info("--------- stopped {}", method);
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

        bkc = new MockBookKeeper(baseClientConf, zkc);
    }

    protected void stopBookKeeper() throws Exception {
        bkc.shutdown();
    }

    protected void stopZooKeeper() throws Exception {
        zkc.shutdown();
    }

}
