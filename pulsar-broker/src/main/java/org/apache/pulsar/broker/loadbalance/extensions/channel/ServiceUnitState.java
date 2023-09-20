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

import java.util.Map;
import java.util.Set;

/**
 * Defines the possible states for service units.
 *
 * @see <a href="https://github.com/apache/pulsar/issues/16691"> Service Unit State Channel </a> for additional details.
 */
public enum ServiceUnitState {

    Init, // initializing the state. no previous state(terminal state)

    Free, // not owned by any broker (semi-terminal state)

    Owned, // owned by a broker (terminal state)

    Assigning, // the ownership is being assigned (e.g. the new ownership is being notified to the target broker)

    Releasing, // the source broker's ownership is being released (e.g. the topic connections are being closed)

    Splitting, // the service unit is in the process of splitting. (e.g. the metadata store is being updated)

    Deleted; // deleted in the system (semi-terminal state)

    private static final Map<ServiceUnitState, Set<ServiceUnitState>> validTransitions = Map.of(
            // (Init -> all states) transitions are required
            // when the topic is compacted in the middle of assign, transfer or split.
            Init, Set.of(Free, Owned, Assigning, Releasing, Splitting, Deleted),
            Free, Set.of(Assigning, Init),
            Owned, Set.of(Splitting, Releasing),
            Assigning, Set.of(Owned),
            Releasing, Set.of(Assigning, Free),
            Splitting, Set.of(Deleted),
            Deleted, Set.of(Init)
    );

    private static final Set<ServiceUnitState> inFlightStates = Set.of(
            Assigning, Releasing, Splitting
    );

    public static boolean isValidTransition(ServiceUnitState from, ServiceUnitState to) {
        Set<ServiceUnitState> transitions = validTransitions.get(from);
        return transitions.contains(to);
    }

    public static boolean isInFlightState(ServiceUnitState state) {
        return inFlightStates.contains(state);
    }

    public static boolean isActiveState(ServiceUnitState state) {
        return inFlightStates.contains(state) || state == Owned;
    }
}
