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

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import org.apache.pulsar.common.policies.data.ResourceQuota;
import org.testng.annotations.Test;

public class SimpleLoadManagerImplQuotaTest {

    @Test
    public void testNeedToUpdateQuotaUsesMatchingBandwidthFields() {
        ResourceQuota oldQuota = new ResourceQuota();
        oldQuota.setMsgRateIn(100);
        oldQuota.setMsgRateOut(100);
        oldQuota.setBandwidthIn(100_000);
        oldQuota.setBandwidthOut(200_000);
        oldQuota.setMemory(100);

        ResourceQuota newQuota = new ResourceQuota();
        newQuota.setMsgRateIn(oldQuota.getMsgRateIn());
        newQuota.setMsgRateOut(oldQuota.getMsgRateOut());
        newQuota.setBandwidthIn(oldQuota.getBandwidthIn() + 1);
        newQuota.setBandwidthOut(oldQuota.getBandwidthOut());
        newQuota.setMemory(oldQuota.getMemory());

        assertFalse(SimpleLoadManagerImpl.needToUpdateQuota(oldQuota, newQuota));

        newQuota.setBandwidthIn(oldQuota.getBandwidthIn() + 10_000);

        assertTrue(SimpleLoadManagerImpl.needToUpdateQuota(oldQuota, newQuota));
    }
}
