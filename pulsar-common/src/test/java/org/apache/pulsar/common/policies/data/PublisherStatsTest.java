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

public class PublisherStatsTest {

    @Test
    public void testPublisherStats() {
        PublisherStats stats = new PublisherStats();
        Assert.assertNull(stats.getAddress());
        Assert.assertNull(stats.getClientVersion());
        Assert.assertNull(stats.getConnectedSince());
        Assert.assertNull(stats.getProducerName());
        
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
        
        stats.setProducerName("producer");
        Assert.assertEquals(stats.getProducerName(), "producer");
        Assert.assertEquals(stats.getConnectedSince(), "connected");
        Assert.assertEquals(stats.getAddress(), "address1");
        Assert.assertEquals(stats.getClientVersion(), "version");
        
        stats.setAddress(null);
        Assert.assertEquals(stats.getAddress(), null);
        
        stats.setConnectedSince("");
        Assert.assertEquals(stats.getConnectedSince(), "");
        
        stats.setClientVersion("version2");
        Assert.assertEquals(stats.getClientVersion(), "version2");
        
        stats.setProducerName(null);
        Assert.assertEquals(stats.getProducerName(), null);
        
        Assert.assertEquals(stats.getAddress(), null);
        
        Assert.assertEquals(stats.getClientVersion(), "version2");
        
        stats.setConnectedSince(null);
        stats.setClientVersion(null);
        Assert.assertNull(stats.getConnectedSince());
        Assert.assertNull(stats.getClientVersion());
    }

    @Test
    public void testPublisherStatsAggregation() {
        PublisherStats stats1 = new PublisherStats();
        stats1.msgRateIn = 1;
        stats1.msgThroughputIn = 1;
        stats1.averageMsgSize = 1;

        PublisherStats stats2 = new PublisherStats();
        stats2.msgRateIn = 1;
        stats2.msgThroughputIn = 2;
        stats2.averageMsgSize = 3;

        PublisherStats target = new PublisherStats();
        target.add(stats1);
        target.add(stats2);

        Assert.assertEquals(target.msgRateIn, 2.0);
        Assert.assertEquals(target.msgThroughputIn, 3.0);
        Assert.assertEquals(target.averageMsgSize, 2.0);
    }

}
