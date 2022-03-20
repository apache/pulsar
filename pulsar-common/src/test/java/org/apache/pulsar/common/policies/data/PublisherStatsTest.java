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
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;
import java.util.Iterator;
import java.util.Set;
import org.apache.pulsar.common.policies.data.stats.PublisherStatsImpl;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.testng.annotations.Test;

public class PublisherStatsTest {

    @Test
    public void testPublisherStats() throws Exception {
        Set<String> allowedFields = Sets.newHashSet(
            "accessMode",
            "msgRateIn",
            "msgThroughputIn",
            "averageMsgSize",
            "chunkedMessageRate",
            "producerId",
            "metadata",
            "address",
            "connectedSince",
            "clientVersion",
            "producerName",
            "supportsPartialProducer"
        );

        PublisherStatsImpl stats = new PublisherStatsImpl();
        assertNull(stats.getAddress());
        assertNull(stats.getClientVersion());
        assertNull(stats.getConnectedSince());
        assertNull(stats.getProducerName());
        
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
        
        stats.setProducerName("producer");
        assertEquals(stats.getProducerName(), "producer");
        assertEquals(stats.getConnectedSince(), "connected");
        assertEquals(stats.getAddress(), "address1");
        assertEquals(stats.getClientVersion(), "version");

        // Check if private fields are included in json
        ObjectMapper mapper = ObjectMapperFactory.create();
        JsonNode node = mapper.readTree(mapper.writer().writeValueAsString(stats));
        Iterator<String> itr = node.fieldNames();
        while (itr.hasNext()) {
            String field = itr.next();
            assertTrue(allowedFields.contains(field), field + " should not be exposed");
        }
        
        stats.setAddress(null);
        assertNull(stats.getAddress());
        
        stats.setConnectedSince("");
        assertEquals(stats.getConnectedSince(), "");
        
        stats.setClientVersion("version2");
        assertEquals(stats.getClientVersion(), "version2");
        
        stats.setProducerName(null);
        assertNull(stats.getProducerName());

        assertNull(stats.getAddress());
        
        assertEquals(stats.getClientVersion(), "version2");
        
        stats.setConnectedSince(null);
        stats.setClientVersion(null);
        assertNull(stats.getConnectedSince());
        assertNull(stats.getClientVersion());
    }

    @Test
    public void testPublisherStatsAggregation() {
        PublisherStatsImpl stats1 = new PublisherStatsImpl();
        stats1.msgRateIn = 1;
        stats1.msgThroughputIn = 1;
        stats1.averageMsgSize = 1;

        PublisherStatsImpl stats2 = new PublisherStatsImpl();
        stats2.msgRateIn = 1;
        stats2.msgThroughputIn = 2;
        stats2.averageMsgSize = 3;

        PublisherStatsImpl target = new PublisherStatsImpl();
        target.add(stats1);
        target.add(stats2);

        assertEquals(target.msgRateIn, 2.0);
        assertEquals(target.msgThroughputIn, 3.0);
        assertEquals(target.averageMsgSize, 2.0);
    }

}
