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

package org.apache.pulsar.broker.intercept;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;

import java.util.TreeSet;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class BrokerInterceptorConfigTest extends MockedPulsarServiceBaseTest {
    @BeforeMethod
    public void setup() throws Exception {
        this.conf.setDisableBrokerInterceptors(false);
        this.enableBrokerInterceptor = false;
        this.mockBrokerInterceptor = false;
        super.internalSetup();
    }

    @Override
    protected void cleanup() throws Exception {
        teardown();
    }

    @AfterMethod(alwaysRun = true)
    public void teardown() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testConfigInterceptorDynamic() {
        PulsarService pulsar = getPulsar();
        BrokerInterceptor interceptor = pulsar.getBrokerInterceptor();
        Assert.assertTrue(interceptor instanceof BrokerInterceptorDelegator);

        TreeSet<String> interceptors = new TreeSet<>();
        interceptors.add("test");

        BrokerInterceptors newInter = Mockito.mock(BrokerInterceptors.class);

        Mockito
                .doAnswer(invocation -> {
                    TreeSet<String> inters = invocation.getArgument(0);
                    pulsar.getConfig().setBrokerInterceptors(inters);

                    BrokerInterceptorDelegator delegator = (BrokerInterceptorDelegator) pulsar.getBrokerInterceptor();
                    BrokerInterceptor oldInter = delegator.getBrokerInterceptor();
                    ((BrokerInterceptor) newInter).initialize(pulsar);
                    delegator.updateBrokerInterceptor(newInter);
                    oldInter.close();
                    return null;
                })
                .when(pulsar).updateBrokerInterceptor(interceptors);

        pulsar.updateBrokerInterceptor(interceptors);

        BrokerInterceptorDelegator delegator = (BrokerInterceptorDelegator) pulsar.getBrokerInterceptor();
        assertSame(delegator.getBrokerInterceptor(), newInter);
        assertEquals(pulsar.getConfig().getBrokerInterceptors().size(), 1);
        assertEquals(pulsar.getConfig().getBrokerInterceptors().stream().findFirst().get(), "test");

    }

    @Test
    public void testConfigInterceptorDisable() {
        PulsarService pulsar = getPulsar();
        BrokerInterceptor interceptor = pulsar.getBrokerInterceptor();

        Assert.assertTrue(interceptor instanceof BrokerInterceptorDelegator);

        TreeSet<String> interceptors = new TreeSet<>();
        pulsar.updateBrokerInterceptor(interceptors);
        BrokerInterceptorDelegator delegator = (BrokerInterceptorDelegator) interceptor;
        Assert.assertSame(delegator.getBrokerInterceptor(), BrokerInterceptor.DISABLED);
    }
}
