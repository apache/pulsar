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
        ResourceGroupService.incRgCalculatedQuota(rgName, publish, b, reportPeriod);
        long rgLocalUsageByteCount = ResourceGroupService.getRgQuotaByteCount(rgName, publish.name());
        long rgQuotaMessageCount = ResourceGroupService.getRgQuotaMessageCount(rgName, publish.name());
        assertEquals(rgLocalUsageByteCount, b.bytes * reportPeriod);
        assertEquals(rgQuotaMessageCount, b.messages * reportPeriod);
    }
}
