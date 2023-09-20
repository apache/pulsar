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
import java.util.Optional;
import org.apache.pulsar.broker.loadbalance.extensions.models.Unload;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class UnloadTest {

    @Test
    public void testConstructors() {

        Unload unload1 = new Unload("A", "B");
        assertEquals(unload1.sourceBroker(), "A");
        assertEquals(unload1.serviceUnit(), "B");
        assertEquals(unload1.destBroker(), Optional.empty());

        Unload unload2 = new Unload("A", "B", Optional.of("C"));
        assertEquals(unload2.sourceBroker(), "A");
        assertEquals(unload2.serviceUnit(), "B");
        assertEquals(unload2.destBroker(), Optional.of("C"));
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testNullSourceBroker() {
        new Unload(null, "A");
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testNullServiceUnit() {
        new Unload("A", null);
    }

}