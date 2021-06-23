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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import org.apache.pulsar.common.policies.data.stats.ConsumerStatsImpl;
import org.testng.annotations.Test;

public class ConsumerStatsTest {

    @Test
    public void testConsumerStats() {
        ConsumerStatsImpl stats = new ConsumerStatsImpl();
        assertNull(stats.getAddress());
        assertNull(stats.getClientVersion());
        assertNull(stats.getConnectedSince());
        
        stats.setAddress("address");
        assertEquals(stats.getAddress(), "address");
        stats.setAddress("address1");
        assertEquals(stats.getAddress(), "address1");
        
        stats.setClientVersion("version");
        assertEquals(stats.getClientVersion(), "version");
        assertEquals(stats.getAddress(), "address1");
        
        stats.setConnectedSince("connected");
        assertEquals(stats.getConnectedSince(), "connected");
        assertEquals(stats.getAddress(), "address1");
        assertEquals(stats.getClientVersion(), "version");
        
        stats.setAddress(null);
        assertNull(stats.getAddress());
        
        stats.setConnectedSince("");
        assertEquals(stats.getConnectedSince(), "");
        
        stats.setClientVersion("version2");
        assertEquals(stats.getClientVersion(), "version2");

        assertNull(stats.getAddress());
        
        assertEquals(stats.getClientVersion(), "version2");
        
        stats.setConnectedSince(null);
        stats.setClientVersion(null);
        assertNull(stats.getConnectedSince());
        assertNull(stats.getClientVersion());
    }
}
