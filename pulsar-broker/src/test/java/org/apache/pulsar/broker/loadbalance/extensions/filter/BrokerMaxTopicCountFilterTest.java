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
package org.apache.pulsar.broker.loadbalance.extensions.filter;

import org.apache.commons.lang.reflect.FieldUtils;
import org.apache.pulsar.broker.loadbalance.BrokerFilterException;
import org.apache.pulsar.broker.loadbalance.extensions.LoadManagerContext;
import org.apache.pulsar.broker.loadbalance.extensions.data.BrokerLoadData;
import org.apache.pulsar.broker.loadbalance.extensions.data.BrokerLookupData;
import org.apache.pulsar.broker.loadbalance.extensions.store.LoadDataStore;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static org.testng.Assert.assertEquals;

/**
 * Unit test for {@link BrokerMaxTopicCountFilter}.
 */
@Test(groups = "broker")
public class BrokerMaxTopicCountFilterTest extends BrokerFilterTestBase {

    @Test
    public void test() throws IllegalAccessException, BrokerFilterException {
        LoadManagerContext context = getContext();
        LoadDataStore<BrokerLoadData> store = context.brokerLoadDataStore();
        BrokerLoadData maxTopicLoadData = new BrokerLoadData();
        FieldUtils.writeDeclaredField(maxTopicLoadData, "topics",
                context.brokerConfiguration().getLoadBalancerBrokerMaxTopics(), true);
        BrokerLoadData exceedMaxTopicLoadData = new BrokerLoadData();
        FieldUtils.writeDeclaredField(exceedMaxTopicLoadData, "topics",
                context.brokerConfiguration().getLoadBalancerBrokerMaxTopics() * 2, true);
        store.pushAsync("broker1", maxTopicLoadData);
        store.pushAsync("broker2", new BrokerLoadData());
        store.pushAsync("broker3", exceedMaxTopicLoadData);

        BrokerMaxTopicCountFilter filter = new BrokerMaxTopicCountFilter();
        Map<String, BrokerLookupData> originalBrokers = Map.of(
                "broker1", getLookupData(),
                "broker2", getLookupData(),
                "broker3", getLookupData(),
                "broker4", getLookupData()
        );
        Map<String, BrokerLookupData> result = filter.filter(new HashMap<>(originalBrokers), context);
        assertEquals(result, Map.of(
                "broker2", getLookupData(),
                "broker4", getLookupData()
        ));
    }

}
