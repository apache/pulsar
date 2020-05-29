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
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertFalse;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;

import org.apache.pulsar.common.policies.data.BrokerAssignment;
import org.apache.pulsar.common.policies.data.NamespaceOwnershipStatus;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.testng.annotations.Test;

public class NamespaceOwnershipStatusTest {

    @Test
    public void testSerialization() throws Exception {
        String jsonStr = "{\"ns-1\":{\"broker_assignment\":\"shared\",\"is_controlled\":false,\"is_active\":true},"
                + "\"ns-2\":{\"broker_assignment\":\"primary\",\"is_controlled\":true,\"is_active\":false},"
                + "\"ns-3\":{\"broker_assignment\":\"secondary\",\"is_controlled\":true,\"is_active\":true}}";
        ObjectMapper jsonMapper = ObjectMapperFactory.create();
        Map<String, NamespaceOwnershipStatus> nsMap = jsonMapper.readValue(jsonStr.getBytes(),
                new TypeReference<Map<String, NamespaceOwnershipStatus>>() {
                });
        assertEquals(nsMap.size(), 3);
        for (String ns : nsMap.keySet()) {
            NamespaceOwnershipStatus nsStatus = nsMap.get(ns);
            if (ns.equals("ns-1")) {
                assertEquals(nsStatus.broker_assignment, BrokerAssignment.shared);
                assertFalse(nsStatus.is_controlled);
                assertTrue(nsStatus.is_active);
            } else if (ns.equals("ns-2")) {
                assertEquals(nsStatus.broker_assignment, BrokerAssignment.primary);
                assertTrue(nsStatus.is_controlled);
                assertFalse(nsStatus.is_active);
            } else if (ns.equals("ns-3")) {
                assertEquals(nsStatus.broker_assignment, BrokerAssignment.secondary);
                assertTrue(nsStatus.is_controlled);
                assertTrue(nsStatus.is_active);
            }
        }
        assertEquals(jsonMapper.writeValueAsString(nsMap), jsonStr);
    }
}
