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
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState.Deleted;
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState.Free;
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState.Init;
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

        assertFalse(ServiceUnitState.isValidTransition(Init, Init));
        assertTrue(ServiceUnitState.isValidTransition(Init, Free));
        assertTrue(ServiceUnitState.isValidTransition(Init, Owned));
        assertTrue(ServiceUnitState.isValidTransition(Init, Assigned));
        assertTrue(ServiceUnitState.isValidTransition(Init, Released));
        assertTrue(ServiceUnitState.isValidTransition(Init, Splitting));
        assertTrue(ServiceUnitState.isValidTransition(Init, Deleted));

        assertTrue(ServiceUnitState.isValidTransition(Free, Init));
        assertFalse(ServiceUnitState.isValidTransition(Free, Free));
        assertFalse(ServiceUnitState.isValidTransition(Free, Owned));
        assertTrue(ServiceUnitState.isValidTransition(Free, Assigned));
        assertFalse(ServiceUnitState.isValidTransition(Free, Released));
        assertFalse(ServiceUnitState.isValidTransition(Free, Splitting));
        assertFalse(ServiceUnitState.isValidTransition(Free, Deleted));

        assertFalse(ServiceUnitState.isValidTransition(Assigned, Init));
        assertFalse(ServiceUnitState.isValidTransition(Assigned, Free));
        assertFalse(ServiceUnitState.isValidTransition(Assigned, Assigned));
        assertTrue(ServiceUnitState.isValidTransition(Assigned, Owned));
        assertTrue(ServiceUnitState.isValidTransition(Assigned, Released));
        assertFalse(ServiceUnitState.isValidTransition(Assigned, Splitting));
        assertFalse(ServiceUnitState.isValidTransition(Assigned, Deleted));

        assertFalse(ServiceUnitState.isValidTransition(Owned, Init));
        assertFalse(ServiceUnitState.isValidTransition(Owned, Free));
        assertTrue(ServiceUnitState.isValidTransition(Owned, Assigned));
        assertFalse(ServiceUnitState.isValidTransition(Owned, Owned));
        assertTrue(ServiceUnitState.isValidTransition(Owned, Released));
        assertTrue(ServiceUnitState.isValidTransition(Owned, Splitting));
        assertFalse(ServiceUnitState.isValidTransition(Owned, Deleted));

        assertFalse(ServiceUnitState.isValidTransition(Released, Init));
        assertTrue(ServiceUnitState.isValidTransition(Released, Free));
        assertFalse(ServiceUnitState.isValidTransition(Released, Assigned));
        assertTrue(ServiceUnitState.isValidTransition(Released, Owned));
        assertFalse(ServiceUnitState.isValidTransition(Released, Released));
        assertFalse(ServiceUnitState.isValidTransition(Released, Splitting));
        assertFalse(ServiceUnitState.isValidTransition(Released, Deleted));

        assertFalse(ServiceUnitState.isValidTransition(Splitting, Init));
        assertFalse(ServiceUnitState.isValidTransition(Splitting, Free));
        assertFalse(ServiceUnitState.isValidTransition(Splitting, Assigned));
        assertFalse(ServiceUnitState.isValidTransition(Splitting, Owned));
        assertFalse(ServiceUnitState.isValidTransition(Splitting, Released));
        assertFalse(ServiceUnitState.isValidTransition(Splitting, Splitting));
        assertTrue(ServiceUnitState.isValidTransition(Splitting, Deleted));

        assertTrue(ServiceUnitState.isValidTransition(Deleted, Init));
        assertFalse(ServiceUnitState.isValidTransition(Deleted, Free));
        assertFalse(ServiceUnitState.isValidTransition(Deleted, Assigned));
        assertFalse(ServiceUnitState.isValidTransition(Deleted, Owned));
        assertFalse(ServiceUnitState.isValidTransition(Deleted, Released));
        assertFalse(ServiceUnitState.isValidTransition(Deleted, Splitting));
        assertFalse(ServiceUnitState.isValidTransition(Deleted, Deleted));
    }

}