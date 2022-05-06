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
package org.apache.pulsar.broker.resourcegroup;


import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class ResourceQuotaCalculatorImplTest extends MockedPulsarServiceBaseTest {
    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        this.rqCalc = new ResourceQuotaCalculatorImpl();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testRQCalcNegativeConfTest() throws PulsarAdminException {
        final long[] allUsage = { 0 };
        long calculatedQuota = this.rqCalc.computeLocalQuota(-1, 0, allUsage);
        long expectedQuota = -1;
        Assert.assertEquals(calculatedQuota, expectedQuota);
    }

    @Test
    public void testRQCalcNegativeLocalUsageTest() {
        final long[] allUsage = { 0 };
        Assert.assertThrows(PulsarAdminException.class, () -> this.rqCalc.computeLocalQuota(0, -1, allUsage));
    }

    @Test
    public void testRQCalcNegativeAllUsageTest() {
        final long[] allUsage = { -1 };
        Assert.assertThrows(PulsarAdminException.class, () -> this.rqCalc.computeLocalQuota(0, 0, allUsage));
    }

    @Test
    public void testRQCalcGlobUsedLessThanConfigTest() throws PulsarAdminException {
        final long config = 100;
        final long localUsed = 20;
        final long[] allUsage = { 40 };
        final long newQuota = this.rqCalc.computeLocalQuota(config, localUsed, allUsage);
        Assert.assertTrue(newQuota > localUsed);
    }

    @Test
    public void testRQCalcGlobUsedEqualsToConfigTest() throws PulsarAdminException {
        final long config = 100;
        final long localUsed = 20;
        final long[] allUsage = { 100 };
        final long newQuota = this.rqCalc.computeLocalQuota(config, localUsed, allUsage);
        Assert.assertEquals(newQuota, localUsed);
    }

    @Test
    public void testRQCalcGlobUsedGreaterThanConfigTest() throws PulsarAdminException {
        final long config = 100;
        final long localUsed = 20;
        final long[] allUsage = { 150 };
        final long newQuota = this.rqCalc.computeLocalQuota(config, localUsed, allUsage);
        Assert.assertTrue(newQuota < localUsed);
    }

    @Test
    public void testRQCalcProportionalIncrementTest() throws PulsarAdminException {
        final long config = 100;
        final long[] allUsage = { 60 };
        final long localUsed1 = 20;
        final long localUsed2 = 40;

        final float initialUsageRatio = (float) localUsed1 / localUsed2;
        final long newQuota1 = this.rqCalc.computeLocalQuota(config, localUsed1, allUsage);
        final long newQuota2 = this.rqCalc.computeLocalQuota(config, localUsed2, allUsage);
        final float proposedUsageRatio = (float) newQuota1 / newQuota2;
        Assert.assertEquals(initialUsageRatio, proposedUsageRatio);
    }

    @Test
    public void testRQCalcGlobUsedZeroTest() throws PulsarAdminException {
        final long config = 10;  // don't care
        final long localUsed = 0;  // don't care
        final long[] allUsage = { 0 };
        final long newQuota = this.rqCalc.computeLocalQuota(config, localUsed, allUsage);
        Assert.assertEquals(newQuota, config);
    }

    @Test
    public void testNeedToReportLocalUsage() {
        // If the percentage change (increase or decrease) in usage is more than 5% for
        // either bytes or messages, send a report.
        Assert.assertFalse(rqCalc.needToReportLocalUsage(1040, 1000, 104, 100, System.currentTimeMillis()));
        Assert.assertFalse(rqCalc.needToReportLocalUsage(950, 1000, 95, 100, System.currentTimeMillis()));
        Assert.assertTrue(rqCalc.needToReportLocalUsage(1060, 1000, 106, 100, System.currentTimeMillis()));
        Assert.assertTrue(rqCalc.needToReportLocalUsage(940, 1000, 94, 100, System.currentTimeMillis()));
    }

    private ResourceQuotaCalculatorImpl rqCalc;
}