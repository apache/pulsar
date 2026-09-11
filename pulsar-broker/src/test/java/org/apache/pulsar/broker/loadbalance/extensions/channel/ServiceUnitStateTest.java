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
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState.Deleted;
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState.Free;
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState.Init;
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState.Owned;
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState.Releasing;
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState.Splitting;
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState.StorageType.MetadataStore;
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState.StorageType.SystemTopic;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class ServiceUnitStateTest {
    @Test
    public void testInFlights() {
        assertFalse(ServiceUnitState.isInFlightState(Init));
        assertFalse(ServiceUnitState.isInFlightState(Free));
        assertFalse(ServiceUnitState.isInFlightState(Owned));
        assertTrue(ServiceUnitState.isInFlightState(Assigning));
        assertTrue(ServiceUnitState.isInFlightState(Releasing));
        assertTrue(ServiceUnitState.isInFlightState(Splitting));
        assertFalse(ServiceUnitState.isInFlightState(Deleted));
    }

    @Test
    public void testActive() {
        assertFalse(ServiceUnitState.isActiveState(Init));
        assertFalse(ServiceUnitState.isActiveState(Free));
        assertTrue(ServiceUnitState.isActiveState(Owned));
        assertTrue(ServiceUnitState.isActiveState(Assigning));
        assertTrue(ServiceUnitState.isActiveState(Releasing));
        assertTrue(ServiceUnitState.isActiveState(Splitting));
        assertFalse(ServiceUnitState.isActiveState(Deleted));
    }

    @Test
    public void testTransitionsOverSystemTopic() {

        assertFalse(ServiceUnitState.isValidTransition(Init, Init, SystemTopic));
        assertTrue(ServiceUnitState.isValidTransition(Init, Free, SystemTopic));
        assertTrue(ServiceUnitState.isValidTransition(Init, Owned, SystemTopic));
        assertTrue(ServiceUnitState.isValidTransition(Init, Assigning, SystemTopic));
        assertTrue(ServiceUnitState.isValidTransition(Init, Releasing, SystemTopic));
        assertTrue(ServiceUnitState.isValidTransition(Init, Splitting, SystemTopic));
        assertTrue(ServiceUnitState.isValidTransition(Init, Deleted, SystemTopic));

        assertTrue(ServiceUnitState.isValidTransition(Free, Init, SystemTopic));
        assertFalse(ServiceUnitState.isValidTransition(Free, Free, SystemTopic));
        assertFalse(ServiceUnitState.isValidTransition(Free, Owned, SystemTopic));
        assertTrue(ServiceUnitState.isValidTransition(Free, Assigning, SystemTopic));
        assertFalse(ServiceUnitState.isValidTransition(Free, Releasing, SystemTopic));
        assertFalse(ServiceUnitState.isValidTransition(Free, Splitting, SystemTopic));
        assertFalse(ServiceUnitState.isValidTransition(Free, Deleted, SystemTopic));

        assertFalse(ServiceUnitState.isValidTransition(Assigning, Init, SystemTopic));
        assertFalse(ServiceUnitState.isValidTransition(Assigning, Free, SystemTopic));
        assertFalse(ServiceUnitState.isValidTransition(Assigning, Assigning, SystemTopic));
        assertTrue(ServiceUnitState.isValidTransition(Assigning, Owned, SystemTopic));
        assertFalse(ServiceUnitState.isValidTransition(Assigning, Releasing, SystemTopic));
        assertFalse(ServiceUnitState.isValidTransition(Assigning, Splitting, SystemTopic));
        assertFalse(ServiceUnitState.isValidTransition(Assigning, Deleted, SystemTopic));

        assertFalse(ServiceUnitState.isValidTransition(Owned, Init, SystemTopic));
        assertFalse(ServiceUnitState.isValidTransition(Owned, Free, SystemTopic));
        assertFalse(ServiceUnitState.isValidTransition(Owned, Assigning, SystemTopic));
        assertFalse(ServiceUnitState.isValidTransition(Owned, Owned, SystemTopic));
        assertTrue(ServiceUnitState.isValidTransition(Owned, Releasing, SystemTopic));
        assertTrue(ServiceUnitState.isValidTransition(Owned, Splitting, SystemTopic));
        assertFalse(ServiceUnitState.isValidTransition(Owned, Deleted, SystemTopic));

        assertFalse(ServiceUnitState.isValidTransition(Releasing, Init, SystemTopic));
        assertTrue(ServiceUnitState.isValidTransition(Releasing, Free, SystemTopic));
        assertTrue(ServiceUnitState.isValidTransition(Releasing, Assigning, SystemTopic));
        assertFalse(ServiceUnitState.isValidTransition(Releasing, Owned, SystemTopic));
        assertFalse(ServiceUnitState.isValidTransition(Releasing, Releasing, SystemTopic));
        assertFalse(ServiceUnitState.isValidTransition(Releasing, Splitting, SystemTopic));
        assertFalse(ServiceUnitState.isValidTransition(Releasing, Deleted, SystemTopic));

        assertFalse(ServiceUnitState.isValidTransition(Splitting, Init, SystemTopic));
        assertFalse(ServiceUnitState.isValidTransition(Splitting, Free, SystemTopic));
        assertFalse(ServiceUnitState.isValidTransition(Splitting, Assigning, SystemTopic));
        assertFalse(ServiceUnitState.isValidTransition(Splitting, Owned, SystemTopic));
        assertFalse(ServiceUnitState.isValidTransition(Splitting, Releasing, SystemTopic));
        assertFalse(ServiceUnitState.isValidTransition(Splitting, Splitting, SystemTopic));
        assertTrue(ServiceUnitState.isValidTransition(Splitting, Deleted, SystemTopic));

        assertTrue(ServiceUnitState.isValidTransition(Deleted, Init, SystemTopic));
        assertFalse(ServiceUnitState.isValidTransition(Deleted, Free, SystemTopic));
        assertFalse(ServiceUnitState.isValidTransition(Deleted, Assigning, SystemTopic));
        assertFalse(ServiceUnitState.isValidTransition(Deleted, Owned, SystemTopic));
        assertFalse(ServiceUnitState.isValidTransition(Deleted, Releasing, SystemTopic));
        assertFalse(ServiceUnitState.isValidTransition(Deleted, Splitting, SystemTopic));
        assertFalse(ServiceUnitState.isValidTransition(Deleted, Deleted, SystemTopic));
    }

    @Test
    public void testTransitionsOverMetadataStore() {

        assertFalse(ServiceUnitState.isValidTransition(Init, Init, MetadataStore));
        assertFalse(ServiceUnitState.isValidTransition(Init, Free, MetadataStore));
        assertFalse(ServiceUnitState.isValidTransition(Init, Owned, MetadataStore));
        assertTrue(ServiceUnitState.isValidTransition(Init, Assigning, MetadataStore));
        assertFalse(ServiceUnitState.isValidTransition(Init, Releasing, MetadataStore));
        assertFalse(ServiceUnitState.isValidTransition(Init, Splitting, MetadataStore));
        assertFalse(ServiceUnitState.isValidTransition(Init, Deleted, MetadataStore));

        assertFalse(ServiceUnitState.isValidTransition(Free, Init, MetadataStore));
        assertFalse(ServiceUnitState.isValidTransition(Free, Free, MetadataStore));
        assertFalse(ServiceUnitState.isValidTransition(Free, Owned, MetadataStore));
        assertTrue(ServiceUnitState.isValidTransition(Free, Assigning, MetadataStore));
        assertFalse(ServiceUnitState.isValidTransition(Free, Releasing, MetadataStore));
        assertFalse(ServiceUnitState.isValidTransition(Free, Splitting, MetadataStore));
        assertFalse(ServiceUnitState.isValidTransition(Free, Deleted, MetadataStore));

        assertFalse(ServiceUnitState.isValidTransition(Assigning, Init, MetadataStore));
        assertFalse(ServiceUnitState.isValidTransition(Assigning, Free, MetadataStore));
        assertFalse(ServiceUnitState.isValidTransition(Assigning, Assigning, MetadataStore));
        assertTrue(ServiceUnitState.isValidTransition(Assigning, Owned, MetadataStore));
        assertFalse(ServiceUnitState.isValidTransition(Assigning, Releasing, MetadataStore));
        assertFalse(ServiceUnitState.isValidTransition(Assigning, Splitting, MetadataStore));
        assertFalse(ServiceUnitState.isValidTransition(Assigning, Deleted, MetadataStore));

        assertFalse(ServiceUnitState.isValidTransition(Owned, Init, MetadataStore));
        assertFalse(ServiceUnitState.isValidTransition(Owned, Free, MetadataStore));
        assertFalse(ServiceUnitState.isValidTransition(Owned, Assigning, MetadataStore));
        assertFalse(ServiceUnitState.isValidTransition(Owned, Owned, MetadataStore));
        assertTrue(ServiceUnitState.isValidTransition(Owned, Releasing, MetadataStore));
        assertTrue(ServiceUnitState.isValidTransition(Owned, Splitting, MetadataStore));
        assertFalse(ServiceUnitState.isValidTransition(Owned, Deleted, MetadataStore));

        assertFalse(ServiceUnitState.isValidTransition(Releasing, Init, MetadataStore));
        assertTrue(ServiceUnitState.isValidTransition(Releasing, Free, MetadataStore));
        assertTrue(ServiceUnitState.isValidTransition(Releasing, Assigning, MetadataStore));
        assertFalse(ServiceUnitState.isValidTransition(Releasing, Owned, MetadataStore));
        assertFalse(ServiceUnitState.isValidTransition(Releasing, Releasing, MetadataStore));
        assertFalse(ServiceUnitState.isValidTransition(Releasing, Splitting, MetadataStore));
        assertFalse(ServiceUnitState.isValidTransition(Releasing, Deleted, MetadataStore));

        assertFalse(ServiceUnitState.isValidTransition(Splitting, Init, MetadataStore));
        assertFalse(ServiceUnitState.isValidTransition(Splitting, Free, MetadataStore));
        assertFalse(ServiceUnitState.isValidTransition(Splitting, Assigning, MetadataStore));
        assertFalse(ServiceUnitState.isValidTransition(Splitting, Owned, MetadataStore));
        assertFalse(ServiceUnitState.isValidTransition(Splitting, Releasing, MetadataStore));
        assertFalse(ServiceUnitState.isValidTransition(Splitting, Splitting, MetadataStore));
        assertTrue(ServiceUnitState.isValidTransition(Splitting, Deleted, MetadataStore));

        assertTrue(ServiceUnitState.isValidTransition(Deleted, Init, MetadataStore));
        assertFalse(ServiceUnitState.isValidTransition(Deleted, Free, MetadataStore));
        assertFalse(ServiceUnitState.isValidTransition(Deleted, Assigning, MetadataStore));
        assertFalse(ServiceUnitState.isValidTransition(Deleted, Owned, MetadataStore));
        assertFalse(ServiceUnitState.isValidTransition(Deleted, Releasing, MetadataStore));
        assertFalse(ServiceUnitState.isValidTransition(Deleted, Splitting, MetadataStore));
        assertFalse(ServiceUnitState.isValidTransition(Deleted, Deleted, MetadataStore));
    }

}