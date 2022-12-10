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

import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState.Assigned;
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState.Owned;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.assertj.core.api.Assertions.assertThat;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class ServiceUnitStateDataTest {

    @Test
    public void testConstructors() throws InterruptedException {
        ServiceUnitStateData data1 = new ServiceUnitStateData(Owned, "A");
        assertEquals(data1.state(), Owned);
        assertEquals(data1.broker(), "A");
        assertNull(data1.sourceBroker());
        assertThat(data1.timestamp()).isGreaterThan(0);;

        Thread.sleep(10);

        ServiceUnitStateData data2 = new ServiceUnitStateData(Assigned, "A", "B");
        assertEquals(data2.state(), Assigned);
        assertEquals(data2.broker(), "A");
        assertEquals(data2.sourceBroker(), "B");
        assertThat(data2.timestamp()).isGreaterThan(data1.timestamp());
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testNullState() {
        new ServiceUnitStateData(null, "A");
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testNullBroker() {
        new ServiceUnitStateData(Owned, null);
    }

    @Test
    public void jsonWriteAndReadTest() throws JsonProcessingException {
        ObjectMapper mapper = ObjectMapperFactory.create();
        final ServiceUnitStateData src = new ServiceUnitStateData(Assigned, "A", "B");
        String json = mapper.writeValueAsString(src);
        ServiceUnitStateData dst = mapper.readValue(json, ServiceUnitStateData.class);
        assertEquals(dst, src);
    }
}