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
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState.Free;
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState.Owned;
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState.Released;
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState.Splitting;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class ServiceUnitStateTest {

    @Test
    public void testTransitions() {

        assertFalse(ServiceUnitState.isValidTransition(Free, Free));
        assertTrue(ServiceUnitState.isValidTransition(Free, Assigned));
        assertTrue(ServiceUnitState.isValidTransition(Free, Owned));
        assertTrue(ServiceUnitState.isValidTransition(Free, Released));
        assertTrue(ServiceUnitState.isValidTransition(Free, Splitting));

        assertTrue(ServiceUnitState.isValidTransition(Assigned, Free));
        assertFalse(ServiceUnitState.isValidTransition(Assigned, Assigned));
        assertTrue(ServiceUnitState.isValidTransition(Assigned, Owned));
        assertTrue(ServiceUnitState.isValidTransition(Assigned, Released));
        assertFalse(ServiceUnitState.isValidTransition(Assigned, Splitting));

        assertTrue(ServiceUnitState.isValidTransition(Owned, Free));
        assertTrue(ServiceUnitState.isValidTransition(Owned, Assigned));
        assertFalse(ServiceUnitState.isValidTransition(Owned, Owned));
        assertFalse(ServiceUnitState.isValidTransition(Owned, Released));
        assertTrue(ServiceUnitState.isValidTransition(Owned, Splitting));

        assertTrue(ServiceUnitState.isValidTransition(Released, Free));
        assertFalse(ServiceUnitState.isValidTransition(Released, Assigned));
        assertTrue(ServiceUnitState.isValidTransition(Released, Owned));
        assertFalse(ServiceUnitState.isValidTransition(Released, Released));
        assertFalse(ServiceUnitState.isValidTransition(Released, Splitting));

        assertTrue(ServiceUnitState.isValidTransition(Splitting, Free));
        assertFalse(ServiceUnitState.isValidTransition(Splitting, Assigned));
        assertFalse(ServiceUnitState.isValidTransition(Splitting, Owned));
        assertFalse(ServiceUnitState.isValidTransition(Splitting, Released));
        assertFalse(ServiceUnitState.isValidTransition(Splitting, Splitting));
    }

}