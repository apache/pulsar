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

import org.apache.commons.lang3.SystemUtils;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.policies.data.loadbalancer.LoadManagerReport;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import static org.testng.Assert.*;

public class LoadReportNetworkLimit extends MockedPulsarServiceBaseTest {

    @BeforeClass
    @Override
    public void setup() throws Exception {
        conf.setLoadBalancerEnabled(true);
        conf.setLoadBalancerOverrideBrokerNicSpeedGbps(5);
        super.internalSetup();
    }

    @AfterClass
    @Override
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void checkLoadReportNicSpeed() throws Exception {
        // Since we have overridden the NIC speed in the configuration, the load report for the broker should always

        LoadManagerReport report = admin.brokerStats().getLoadReport();

        if (SystemUtils.IS_OS_LINUX) {
            assertEquals(report.getBandwidthIn().limit, 5.0 * 1024 * 1024);
            assertEquals(report.getBandwidthOut().limit, 5.0 * 1024 * 1024);
        } else {
            // On non-Linux system we don't report the network usage
            assertEquals(report.getBandwidthIn().limit, -1.0);
            assertEquals(report.getBandwidthOut().limit, -1.0);
        }
    }

}
