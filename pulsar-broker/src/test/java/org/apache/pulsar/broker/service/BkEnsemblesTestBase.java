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

import java.util.Optional;
import java.util.Properties;

import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.tests.TestRetrySupport;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

import com.google.common.collect.Sets;

import lombok.extern.slf4j.Slf4j;

/**
 * Test base for tests requires a bk ensemble.
 */
@Slf4j
public abstract class BkEnsemblesTestBase extends TestRetrySupport {

    protected PulsarService pulsar;
    protected ServiceConfiguration config;

    protected PulsarAdmin admin;

    protected LocalBookkeeperEnsemble bkEnsemble;

    private final int numberOfBookies;

    public BkEnsemblesTestBase() {
        this(3);
    }

    public BkEnsemblesTestBase(int numberOfBookies) {
        this.numberOfBookies = numberOfBookies;
    }

    protected void configurePulsar(ServiceConfiguration config) throws Exception {
        //overridable by subclasses
    }

    @Override
    @BeforeMethod(alwaysRun = true)
    protected void setup() throws Exception {
        incrementSetupNumber();
        try {
            // start local bookie and zookeeper
            bkEnsemble = new LocalBookkeeperEnsemble(numberOfBookies, 0, () -> 0);
            bkEnsemble.start();

            // start pulsar service
            config = new ServiceConfiguration();
            config.setZookeeperServers("127.0.0.1" + ":" + bkEnsemble.getZookeeperPort());
            config.setAdvertisedAddress("localhost");
            config.setWebServicePort(Optional.of(0));
            config.setClusterName("usc");
            config.setBrokerShutdownTimeoutMs(0L);
            config.setBrokerServicePort(Optional.of(0));
            config.setAuthorizationEnabled(false);
            config.setAuthenticationEnabled(false);
            config.setManagedLedgerMaxEntriesPerLedger(5);
            config.setManagedLedgerMinLedgerRolloverTimeMinutes(0);
            config.setAdvertisedAddress("127.0.0.1");
            config.setAllowAutoTopicCreationType("non-partitioned");
            config.setZooKeeperOperationTimeoutSeconds(10);
            config.setNumIOThreads(1);
            Properties properties = new Properties();
            properties.put("bookkeeper_numWorkerThreads", "1");
            config.setProperties(properties);
            configurePulsar(config);

            pulsar = new PulsarService(config);
            pulsar.start();

            admin = PulsarAdmin.builder().serviceHttpUrl(pulsar.getWebServiceAddress()).build();

            admin.clusters().createCluster("usc", ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());
            admin.tenants().createTenant("prop",
                    new TenantInfoImpl(Sets.newHashSet("appid1"), Sets.newHashSet("usc")));
        } catch (Throwable t) {
            log.error("Error setting up broker test", t);
            Assert.fail("Broker test setup failed");
        }
    }

    @Override
    @AfterMethod(alwaysRun = true)
    protected void cleanup() throws Exception {
        markCurrentSetupNumberCleaned();
        admin.close();
        pulsar.close();
        bkEnsemble.stop();
    }

}
