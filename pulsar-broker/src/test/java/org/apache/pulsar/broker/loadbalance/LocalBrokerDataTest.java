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

import static org.testng.Assert.assertFalse;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.policies.data.loadbalancer.LocalBrokerData;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker")
public class LocalBrokerDataTest {
    /*
    Ensuming that there no bundleStats field in the json string serialized from LocalBrokerData.
     */
    @Test
    public void testSerializeLocalBrokerData() throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        LocalBrokerData localBrokerData = new LocalBrokerData();
        assertFalse(objectMapper.writeValueAsString(localBrokerData).contains("bundleStats"));
    }
}
