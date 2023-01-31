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
 * The following diagram defines the valid state changes
 *
 *                  ┌───────────┐
 *       ┌──────────┤ released  │◄────────┐
 *       │own       └───────────┘         │release
 *       │                                │
 *       │                                │
 *       ▼                                │
 *    ┌────────┐  assign(transfer)  ┌─────┴────┐
 *    │        ├───────────────────►│          │
 *    │ owned  │                    │ assigned │
 *    │        │◄───────────────────┤          │
 *    └──┬─────┤      own           └──────────┘
 *       │  ▲  │                         ▲
 *       │  │  │                         │
 *       │  │  └──────────────┐          │
 *       │  │                 │          │
 *       │  │        unload   │          │ assign(assignment)
 * split │  │                 │          │
 *       │  │                 │          │
 *       │  │ create(child)   │          │
 *       │  │                 │          │
 *       ▼  │                 │          │
 *    ┌─────┴─────┐           └─────►┌───┴──────┐
 *    │           │                  │          │
 *    │ splitting ├────────────────► │   free   │
 *    │           │   discard(parent)│          │
 *    └───────────┘                  └──────────┘
 */
public enum ServiceUnitState {

    Free, // not owned by any broker (terminal state)

    Owned, // owned by a broker (terminal state)

    Assigned, // the ownership is assigned(but the assigned broker has not been notified the ownership yet)

    Released, // the source broker's ownership has been released (e.g. the topic connections are closed)

    Splitting; // the service unit(e.g. bundle) is in the process of splitting.

    private static Map<ServiceUnitState, Set<ServiceUnitState>> validTransitions = Map.of(
            // (Free -> Released | Splitting) transitions are required
            // when the topic is compacted in the middle of transfer or split.
            Free, Set.of(Owned, Assigned, Released, Splitting),
            Owned, Set.of(Assigned, Splitting, Free),
            Assigned, Set.of(Owned, Released, Free),
            Released, Set.of(Owned, Free),
            Splitting, Set.of(Free)
    );

    public static boolean isValidTransition(ServiceUnitState from, ServiceUnitState to) {
        Set<ServiceUnitState> transitions = validTransitions.get(from);
        return transitions.contains(to);
    }

}
