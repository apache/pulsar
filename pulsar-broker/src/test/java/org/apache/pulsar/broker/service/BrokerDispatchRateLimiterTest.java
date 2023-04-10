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
package org.apache.pulsar.broker.service;

import static org.testng.Assert.assertEquals;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class BrokerDispatchRateLimiterTest extends BrokerTestBase {
    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.baseSetup();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testUpdateBrokerDispatchRateLimiter() throws PulsarAdminException {
        BrokerService service = pulsar.getBrokerService();
        assertEquals(service.getBrokerDispatchRateLimiter().getAvailableDispatchRateLimitOnByte(), -1L);
        assertEquals(service.getBrokerDispatchRateLimiter().getAvailableDispatchRateLimitOnMsg(), -1L);

        admin.brokers().updateDynamicConfiguration("dispatchThrottlingRateInByte", "100", "cluster");
        Awaitility.await().untilAsserted(() ->
            assertEquals(service.getBrokerDispatchRateLimiter().getAvailableDispatchRateLimitOnByte(), 100L));
        assertEquals(service.getBrokerDispatchRateLimiter().getAvailableDispatchRateLimitOnMsg(), -1L);

        admin.brokers().updateDynamicConfiguration("dispatchThrottlingRateInMsg", "100", "cluster");
        Awaitility.await().untilAsserted(() ->
            assertEquals(service.getBrokerDispatchRateLimiter().getAvailableDispatchRateLimitOnMsg(), 100L));
        assertEquals(service.getBrokerDispatchRateLimiter().getAvailableDispatchRateLimitOnMsg(), 100L);
    }

}
