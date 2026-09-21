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
package org.apache.pulsar.broker.loadbalance.extensions.channel;

import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState.Assigning;
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState.Owned;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNull;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import java.util.Optional;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class ServiceUnitStateDataTest {

    @Test
    public void testConstructors() throws InterruptedException {
        ServiceUnitStateData data1 = new ServiceUnitStateData(Owned, "A", 1);
        assertEquals(data1.state(), Owned);
        assertEquals(data1.dstBroker(), "A");
        assertNull(data1.sourceBroker());
        assertThat(data1.timestamp()).isGreaterThan(0);

        Thread.sleep(10);

        ServiceUnitStateData data2 = new ServiceUnitStateData(Assigning, "A", "B", 1);
        assertEquals(data2.state(), Assigning);
        assertEquals(data2.dstBroker(), "A");
        assertEquals(data2.sourceBroker(), "B");
        assertThat(data2.timestamp()).isGreaterThan(data1.timestamp());
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testNullState() {
        new ServiceUnitStateData(null, "A", 1);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testNullBrokers() {
        new ServiceUnitStateData(Owned, null, null, 1);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testEmptyBrokers() {
        new ServiceUnitStateData(Owned, "", "", 1);
    }

    @Test
    public void testZeroVersionId() {
        new ServiceUnitStateData(Owned, "A", Long.MAX_VALUE + 1);
    }

    @Test
    public void jsonWriteAndReadTest() throws JsonProcessingException {
        ObjectMapper mapper = ObjectMapperFactory.create();
        final ServiceUnitStateData src = new ServiceUnitStateData(Assigning, "A", "B", 1);
        String json = mapper.writeValueAsString(src);
        ServiceUnitStateData dst = mapper.readValue(json, ServiceUnitStateData.class);
        assertEquals(dst, src);
    }

    @Test
    public void equalsTest() {
        // record ServiceUnitStateData(
        //        ServiceUnitState state, String dstBroker, String sourceBroker,
        //       Map<String, Optional<String>> splitServiceUnitToDestBroker, boolean force,
        //       long timestamp, long versionId) {
        var d1 = new ServiceUnitStateData(Assigning, "A", "B", Map.of("A", Optional.of("B")), true, 0, 1);
        var d2 = new ServiceUnitStateData(Assigning, "A", "B", Map.of("A", Optional.of("B")), true, 0, 1);
        assertEquals(d1, d2);
        var d3 = new ServiceUnitStateData(Assigning, "C", "B", 1);
        var d4 = new ServiceUnitStateData(Assigning, "A", "B", Map.of("A", Optional.of("C")), true, 0, 1);
        assertNotEquals(d1, d3);
        assertNotEquals(d1, d4);

        var d5 = new ServiceUnitStateData(Assigning, "A", "B", Map.of("A", Optional.of("B")), true, 0, 2);
        assertNotEquals(d1, d5);

        var d6 = new ServiceUnitStateData(Assigning, "C", "B", Map.of("A", Optional.of("B")), true, 0, 1);
        assertNotEquals(d1, d6);
    }
}