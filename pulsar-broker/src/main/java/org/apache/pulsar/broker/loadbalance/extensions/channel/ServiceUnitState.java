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
 * Refer to Service Unit State Channel in https://github.com/apache/pulsar/issues/16691 for additional details.
 */
public enum ServiceUnitState {

    Init, // initializing the state. no previous state(terminal state)

    Disabled, // disabled by the owner broker

    Free, // not owned by any broker (semi-terminal state)

    Owned, // owned by a broker (terminal state)

    Assigned, // the ownership is assigned(but the assigned broker has not been notified the ownership yet)

    Released, // the source broker's ownership has been released (e.g. the topic connections are closed)

    Splitting, // the service unit(e.g. bundle) is in the process of splitting.

    Deleted; // deleted in the system (semi-terminal state)

    private static Map<ServiceUnitState, Set<ServiceUnitState>> validTransitions = Map.of(
            // (Init -> all states) transitions are required
            // when the topic is compacted in the middle of assign, transfer or split.
            Init, Set.of(Disabled, Owned, Assigned, Released, Splitting, Deleted, Init),
            Disabled, Set.of(Free, Init),
            Free, Set.of(Assigned, Init),
            Owned, Set.of(Assigned, Splitting, Disabled, Init),
            Assigned, Set.of(Owned, Released, Init),
            Released, Set.of(Owned, Init),
            Splitting, Set.of(Deleted, Init),
            Deleted, Set.of(Init)
    );

    public static boolean isValidTransition(ServiceUnitState from, ServiceUnitState to) {
        Set<ServiceUnitState> transitions = validTransitions.get(from);
        return transitions.contains(to);
    }

}
