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
package org.apache.pulsar.common.policies.data;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ConsumerStatsTest {

    @Test
    public void testConsumerStats() {
        ConsumerStats stats = new ConsumerStats();
        Assert.assertNull(stats.getAddress());
        Assert.assertNull(stats.getClientVersion());
        Assert.assertNull(stats.getConnectedSince());
        
        stats.setAddress("address");
        Assert.assertEquals(stats.getAddress(), "address");
        stats.setAddress("address1");
        Assert.assertEquals(stats.getAddress(), "address1");
        
        stats.setClientVersion("version");
        Assert.assertEquals(stats.getClientVersion(), "version");
        Assert.assertEquals(stats.getAddress(), "address1");
        
        stats.setConnectedSince("connected");
        Assert.assertEquals(stats.getConnectedSince(), "connected");
        Assert.assertEquals(stats.getAddress(), "address1");
        Assert.assertEquals(stats.getClientVersion(), "version");
        
        stats.setAddress(null);
        Assert.assertEquals(stats.getAddress(), null);
        
        stats.setConnectedSince("");
        Assert.assertEquals(stats.getConnectedSince(), "");
        
        stats.setClientVersion("version2");
        Assert.assertEquals(stats.getClientVersion(), "version2");
        
        Assert.assertEquals(stats.getAddress(), null);
        
        Assert.assertEquals(stats.getClientVersion(), "version2");
        
        stats.setConnectedSince(null);
        stats.setClientVersion(null);
        Assert.assertNull(stats.getConnectedSince());
        Assert.assertNull(stats.getClientVersion());
    }
}
