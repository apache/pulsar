/**
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
package org.apache.pulsar.broker.loadbalance;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.apache.pulsar.metadata.api.coordination.CoordinationService;
import org.apache.pulsar.metadata.api.coordination.LeaderElection;
import org.apache.pulsar.metadata.api.coordination.LeaderElectionState;

/**
 * A class that provides way to elect the leader among brokers.
 */
public class LeaderElectionService implements AutoCloseable {

    private static final String ELECTION_ROOT = "/loadbalance/leader";

    private final LeaderElection<LeaderBroker> leaderElection;
    private final LeaderBroker localValue;

    public LeaderElectionService(CoordinationService cs, String localWebServiceAddress,
            Consumer<LeaderElectionState> listener) {
        this.leaderElection = cs.getLeaderElection(LeaderBroker.class, ELECTION_ROOT, listener);
        this.localValue = new LeaderBroker(localWebServiceAddress);
    }

    public void start() {
        leaderElection.elect(localValue).join();
    }

    public void close() throws Exception {
        leaderElection.close();
    }

    public CompletableFuture<Optional<LeaderBroker>> readCurrentLeader() {
        return leaderElection.getLeaderValue();
    }

    public Optional<LeaderBroker> getCurrentLeader() {
        return leaderElection.getLeaderValueIfPresent();
    }

    public boolean isLeader() {
        return leaderElection.getState() == LeaderElectionState.Leading;
    }
}
