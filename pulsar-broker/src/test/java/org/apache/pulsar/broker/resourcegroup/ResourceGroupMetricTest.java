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
package org.apache.pulsar.broker.resourcegroup;

import static org.testng.Assert.assertEquals;
import org.apache.pulsar.broker.resourcegroup.ResourceGroup.BytesAndMessagesCount;
import org.apache.pulsar.broker.resourcegroup.ResourceGroup.ResourceGroupMonitoringClass;
import org.testng.annotations.Test;

public class ResourceGroupMetricTest {
    @Test
    public void testLocalQuotaMetric() {
        String rgName = "my-resource-group";
        ResourceGroupMonitoringClass publish = ResourceGroupMonitoringClass.Publish;
        int reportPeriod = 2;
        BytesAndMessagesCount b = new BytesAndMessagesCount();
        b.messages = 10;
        b.bytes = 20;
        int incTimes = 2;
        for (int i = 0; i < 2; i++) {
            ResourceGroupService.incRgCalculatedQuota(rgName, publish, b, reportPeriod);
        }
        double rgLocalUsageByteCount = ResourceGroupService.getRgQuotaByteCount(rgName, publish.name());
        double rgQuotaMessageCount = ResourceGroupService.getRgQuotaMessageCount(rgName, publish.name());
        assertEquals(rgLocalUsageByteCount, incTimes * b.bytes * reportPeriod);
        assertEquals(rgQuotaMessageCount, incTimes * b.messages * reportPeriod);

        double rgLocalUsageByte = ResourceGroupService.getRgQuotaByte(rgName, publish.name());
        double rgQuotaMessage = ResourceGroupService.getRgQuotaMessage(rgName, publish.name());
        assertEquals(rgLocalUsageByte, b.bytes);
        assertEquals(rgQuotaMessage, b.messages);
    }
}