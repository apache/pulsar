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
package org.apache.pulsar.broker.loadbalance.extensions.channel.models;

import static org.testng.Assert.assertEquals;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.pulsar.broker.loadbalance.extensions.models.Split;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class SplitTest {

    @Test
    public void testConstructor() {
        Map<String, Optional<String>> map = new HashMap<>();
        map.put("C", Optional.of("test"));

        Split split = new Split("A", "B", map);
        assertEquals(split.serviceUnit(), "A");
        assertEquals(split.sourceBroker(), "B");
        assertEquals(split.splitServiceUnitToDestBroker(), map);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testNullBundle() {
        new Split(null, "A", Map.of());
    }

}