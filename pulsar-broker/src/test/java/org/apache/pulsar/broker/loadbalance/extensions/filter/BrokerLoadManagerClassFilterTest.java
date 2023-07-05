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

import static org.testng.Assert.assertEquals;
import org.apache.pulsar.broker.loadbalance.BrokerFilterException;
import org.apache.pulsar.broker.loadbalance.extensions.ExtensibleLoadManagerImpl;
import org.apache.pulsar.broker.loadbalance.extensions.LoadManagerContext;
import org.apache.pulsar.broker.loadbalance.extensions.data.BrokerLookupData;
import org.apache.pulsar.broker.loadbalance.impl.ModularLoadManagerImpl;
import org.testng.annotations.Test;
import java.util.HashMap;
import java.util.Map;


/**
 * Unit test for {@link BrokerLoadManagerClassFilter}.
 */
public class BrokerLoadManagerClassFilterTest extends BrokerFilterTestBase {

    @Test
    public void test() throws BrokerFilterException {
        LoadManagerContext context = getContext();
        context.brokerConfiguration().setLoadManagerClassName(ExtensibleLoadManagerImpl.class.getName());
        BrokerLoadManagerClassFilter filter = new BrokerLoadManagerClassFilter();

        Map<String, BrokerLookupData> originalBrokers = Map.of(
                "broker1", getLookupData("3.0.0", ExtensibleLoadManagerImpl.class.getName()),
                "broker2", getLookupData("3.0.0", ExtensibleLoadManagerImpl.class.getName()),
                "broker3", getLookupData("3.0.0", ModularLoadManagerImpl.class.getName()),
                "broker4", getLookupData("3.0.0", ModularLoadManagerImpl.class.getName()),
                "broker5", getLookupData("3.0.0", null)
        );

        Map<String, BrokerLookupData> result = filter.filter(new HashMap<>(originalBrokers), null, context);
        assertEquals(result, Map.of(
                "broker1", getLookupData("3.0.0", ExtensibleLoadManagerImpl.class.getName()),
                "broker2", getLookupData("3.0.0", ExtensibleLoadManagerImpl.class.getName())
        ));

        context.brokerConfiguration().setLoadManagerClassName(ModularLoadManagerImpl.class.getName());
        result = filter.filter(new HashMap<>(originalBrokers), null, context);

        assertEquals(result, Map.of(
                "broker3", getLookupData("3.0.0", ModularLoadManagerImpl.class.getName()),
                "broker4", getLookupData("3.0.0", ModularLoadManagerImpl.class.getName())
        ));

    }
}
