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
package org.apache.pulsar.broker.loadbalance;

import java.util.Set;
import java.util.TreeSet;

import org.apache.pulsar.broker.BrokerData;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.impl.BrokerVersionFilter;
import org.apache.pulsar.policies.data.loadbalancer.LocalBrokerData;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.github.zafarkhaja.semver.Version;

@Test(groups = "broker")
public class BrokerVersionFilterTest {

    @Test
    public void testLatestVersion() {
        LoadData loadData = initLoadData();
        Set<String> brokers = new TreeSet<>();
        brokers.add("broker1");
        brokers.add("broker2");
        brokers.add("broker3");

        BrokerVersionFilter filter = new BrokerVersionFilter();
        try {
            Version latestVersion = filter.getLatestVersionNumber(brokers, loadData);
            Assert.assertEquals(latestVersion.getMajorVersion(), 1);
            Assert.assertEquals(latestVersion.getMinorVersion(), 2);
            Assert.assertEquals(latestVersion.getPatchVersion(), 0);
        } catch (BrokerFilterBadVersionException bad) {
            Assert.fail(bad.getMessage(), bad);
        }

        ServiceConfiguration configuration = new ServiceConfiguration();
        configuration.setPreferLaterVersions(true);
        try {
            filter.filter(brokers, null, loadData, configuration);
            // Only one broker is running the latest version
            Assert.assertEquals(brokers.size(), 1);
        } catch (BrokerFilterBadVersionException bad) {
            Assert.fail(bad.getMessage(), bad);
        }
    }

    private LoadData initLoadData() {
        LocalBrokerData broker1Data = new LocalBrokerData();
        broker1Data.setBrokerVersionString("1.1.0-SNAPSHOT");

        LocalBrokerData broker2Data = new LocalBrokerData();
        broker2Data.setBrokerVersionString("1.1.0-SNAPSHOT");

        LocalBrokerData broker3Data = new LocalBrokerData();
        broker3Data.setBrokerVersionString("1.2.0-SNAPSHOT");

        LoadData loadData = new LoadData();
        loadData.getBrokerData().put("broker1", new BrokerData(broker1Data));
        loadData.getBrokerData().put("broker2", new BrokerData(broker2Data));
        loadData.getBrokerData().put("broker3", new BrokerData(broker3Data));

        return loadData;
    }
}