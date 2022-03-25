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
package org.apache.pulsar.metadata.bookkeeper;

import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.meta.LedgerAuditorManager;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.util.BookKeeperConstants;
import org.apache.pulsar.metadata.api.coordination.CoordinationService;
import org.apache.pulsar.metadata.api.coordination.LeaderElection;
import org.apache.pulsar.metadata.api.coordination.LeaderElectionState;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.coordination.impl.CoordinationServiceImpl;

@Slf4j
class PulsarLedgerAuditorManager implements LedgerAuditorManager {

    private static final String ELECTION_PATH = "leader";

    private final CoordinationService coordinationService;
    private final LeaderElection<String> leaderElection;
    private LeaderElectionState leaderElectionState;
    private String bookieId;

    PulsarLedgerAuditorManager(MetadataStoreExtended store, String ledgersRoot) {
        this.coordinationService = new CoordinationServiceImpl(store);
        String electionPath = ledgersRoot + "/" + BookKeeperConstants.UNDER_REPLICATION_NODE
                + "/" + ELECTION_PATH;

        this.leaderElection =
                coordinationService.getLeaderElection(String.class, electionPath, this::handleStateChanges);
        this.leaderElectionState = LeaderElectionState.NoLeader;
    }

    private void handleStateChanges(LeaderElectionState state) {
        log.info("Auditor leader election state: {} -- BookieId: {}", state, bookieId);

        synchronized (this) {
            this.leaderElectionState = state;
            notifyAll();
        }
    }

    @Override
    public void tryToBecomeAuditor(String bookieId, Consumer<AuditorEvent> listener) {
        this.bookieId = bookieId;

        LeaderElectionState les = leaderElection.elect(bookieId).join();

        synchronized (this) {
            leaderElectionState = les;
        }

        while (true) {
            try {
                synchronized (this) {
                    if (leaderElectionState == LeaderElectionState.Leading) {
                        return;
                    } else {
                        wait();
                    }
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public BookieId getCurrentAuditor() {
        return leaderElection.getLeaderValue()
                .join()
                .map(BookieId::parse)
                .orElse(null);
    }

    @Override
    public void close() throws Exception {
        leaderElection.close();
        coordinationService.close();
    }
}
