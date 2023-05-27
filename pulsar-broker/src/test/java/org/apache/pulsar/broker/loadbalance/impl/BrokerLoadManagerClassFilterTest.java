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
package org.apache.pulsar.broker.loadbalance.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.BrokerFilterException;
import org.apache.pulsar.broker.loadbalance.LoadData;
import org.apache.pulsar.broker.loadbalance.extensions.ExtensibleLoadManagerImpl;
import org.apache.pulsar.policies.data.loadbalancer.BrokerData;
import org.apache.pulsar.policies.data.loadbalancer.LocalBrokerData;
import org.testng.annotations.Test;
import java.util.HashSet;
import java.util.Set;

/**
 * Unit test for {@link BrokerLoadManagerClassFilter}.
 */
public class BrokerLoadManagerClassFilterTest {

    @Test
    public void test() throws BrokerFilterException {
        BrokerLoadManagerClassFilter filter = new BrokerLoadManagerClassFilter();

        LoadData loadData = new LoadData();
        LocalBrokerData localBrokerData = new LocalBrokerData();
        localBrokerData.setLoadManagerClassName(ModularLoadManagerImpl.class.getName());

        LocalBrokerData localBrokerData1 = new LocalBrokerData();
        localBrokerData1.setLoadManagerClassName(ExtensibleLoadManagerImpl.class.getName());

        LocalBrokerData localBrokerData2 = new LocalBrokerData();
        localBrokerData2.setLoadManagerClassName(null);
        loadData.getBrokerData().put("broker1", new BrokerData(localBrokerData));
        loadData.getBrokerData().put("broker2", new BrokerData(localBrokerData1));
        loadData.getBrokerData().put("broker3", new BrokerData(localBrokerData2));

        ServiceConfiguration conf = new ServiceConfiguration();
        conf.setLoadManagerClassName(ModularLoadManagerImpl.class.getName());

        Set<String> brokers = new HashSet<>(){{
            add("broker1");
            add("broker2");
            add("broker3");
        }};
        filter.filter(brokers, null, loadData, conf);

        assertEquals(brokers.size(), 1);
        assertTrue(brokers.contains("broker1"));

        brokers = new HashSet<>(){{
            add("broker1");
            add("broker2");
            add("broker3");
        }};
        conf.setLoadManagerClassName(ExtensibleLoadManagerImpl.class.getName());
        filter.filter(brokers, null, loadData, conf);
        assertEquals(brokers.size(), 1);
        assertTrue(brokers.contains("broker2"));
    }
}
