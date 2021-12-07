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

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.apache.zookeeper.data.Stat;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class AdvertisedAddressTest {

    LocalBookkeeperEnsemble bkEnsemble;
    PulsarService pulsar;

    private final String advertisedAddress = "pulsar-usc.example.com";

    @BeforeMethod
    public void setup() throws Exception {
        bkEnsemble = new LocalBookkeeperEnsemble(1, 0, () -> 0);
        bkEnsemble.start();

        ServiceConfiguration config = new ServiceConfiguration();
        config.setBrokerShutdownTimeoutMs(0L);
        config.setZookeeperServers("127.0.0.1" + ":" + bkEnsemble.getZookeeperPort());
        config.setWebServicePort(Optional.ofNullable(0));
        config.setClusterName("usc");
        config.setAdvertisedAddress("localhost");
        config.setBrokerServicePort(Optional.ofNullable(0));
        config.setAdvertisedAddress(advertisedAddress);
        config.setManagedLedgerDefaultEnsembleSize(1);
        config.setManagedLedgerDefaultWriteQuorum(1);
        config.setManagedLedgerDefaultAckQuorum(1);
        config.setManagedLedgerMaxEntriesPerLedger(5);
        config.setManagedLedgerMinLedgerRolloverTimeMinutes(0);
        pulsar = new PulsarService(config);
        pulsar.start();
    }

    @AfterMethod(alwaysRun = true)
    public void shutdown() throws Exception {
        pulsar.close();
        bkEnsemble.stop();
    }

    @Test
    public void testAdvertisedAddress() throws Exception {
        Assert.assertEquals( pulsar.getAdvertisedAddress(), advertisedAddress );
        Assert.assertEquals( pulsar.getBrokerServiceUrl(), String.format("pulsar://%s:%d", advertisedAddress, pulsar.getBrokerListenPort().get()) );
        Assert.assertEquals( pulsar.getSafeWebServiceAddress(), String.format("http://%s:%d", advertisedAddress, pulsar.getListenPortHTTP().get()) );
        String brokerZkPath = String.format("/loadbalance/brokers/%s:%d", pulsar.getAdvertisedAddress(), pulsar.getListenPortHTTP().get());
        String bkBrokerData = new String(bkEnsemble.getZkClient().getData(brokerZkPath, false, new Stat()), StandardCharsets.UTF_8);
        JsonObject jsonBkBrokerData = new Gson().fromJson(bkBrokerData, JsonObject.class);
        Assert.assertEquals( jsonBkBrokerData.get("pulsarServiceUrl").getAsString(), pulsar.getBrokerServiceUrl() );
        Assert.assertEquals( jsonBkBrokerData.get("webServiceUrl").getAsString(), pulsar.getSafeWebServiceAddress() );
    }

}
