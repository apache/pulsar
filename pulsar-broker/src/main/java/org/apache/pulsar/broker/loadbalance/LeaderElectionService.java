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

import static com.google.common.base.Preconditions.checkState;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.bookkeeper.util.ZkUtils;
import org.apache.pulsar.broker.PulsarService;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * A class that provides way to elect the leader among brokers.
 *
 *
 */
public class LeaderElectionService {

    private static final Logger log = LoggerFactory.getLogger(LeaderElectionService.class);
    private static final String ELECTION_ROOT = "/loadbalance/leader";

    private final PulsarService pulsar;
    private final ExecutorService executor;

    private boolean stopped = true;

    private final ZooKeeper zkClient;

    private final AtomicReference<LeaderBroker> currentLeader = new AtomicReference<LeaderBroker>();
    private final AtomicBoolean isLeader = new AtomicBoolean();

    private final ObjectMapper jsonMapper;

    /**
     * Interface which should be implemented by classes which are interested in the leader election. The listener gets
     * called when current broker becomes the leader.
     */
    public static interface LeaderListener {
        void brokerIsTheLeaderNow();

        void brokerIsAFollowerNow();
    }

    private final LeaderListener leaderListener;

    public LeaderElectionService(PulsarService pulsar, LeaderListener leaderListener) {
        this.pulsar = pulsar;
        this.zkClient = pulsar.getZkClient();
        this.executor = pulsar.getExecutor();
        this.leaderListener = leaderListener;
        this.jsonMapper = new ObjectMapper();
    }

    /**
     * We try to get the data in the ELECTION_ROOT node. If the node is present (i.e. leader is present), we store it in
     * the currentLeader and keep a watch on the election node. If we lose the leader, then watch gets triggered and we
     * do the election again. If the node does not exist while getting the data, we get NoNodeException. This means,
     * there is no leader and we create the node at ELECTION_ROOT and write the leader broker's service URL in the node.
     * Once the leader is known, we call the listener method so that leader can take further actions.
     */
    private void elect() {
        try {
            byte[] data = zkClient.getData(ELECTION_ROOT, new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    log.warn("Type of the event is [{}] and path is [{}]", event.getType(), event.getPath());
                    switch (event.getType()) {
                    case NodeDeleted:
                        log.warn("Election node {} is deleted, attempting re-election...", event.getPath());
                        if (event.getPath().equals(ELECTION_ROOT)) {
                            log.info("This should call elect again...");
                            executor.execute(() -> {
                                // If the node is deleted, attempt the re-election
                                log.info("Broker [{}] is calling re-election from the thread",
                                        pulsar.getSafeWebServiceAddress());
                                elect();
                            });
                        }
                        break;

                    default:
                        log.warn("Got something wrong on watch: {}", event);
                        break;
                    }
                }
            }, null);

            LeaderBroker leaderBroker = jsonMapper.readValue(data, LeaderBroker.class);
            currentLeader.set(leaderBroker);
            isLeader.set(false);
            leaderListener.brokerIsAFollowerNow();

            // If broker comes here it is a follower. Do nothing, wait for the watch to trigger
            log.info("Broker [{}] is the follower now. Waiting for the watch to trigger...",
                    pulsar.getSafeWebServiceAddress());

        } catch (NoNodeException nne) {
            // There's no leader yet... try to become the leader
            try {
                // Create the root node and add current broker's URL as its contents
                LeaderBroker leaderBroker = new LeaderBroker(pulsar.getSafeWebServiceAddress());
                ZkUtils.createFullPathOptimistic(pulsar.getLocalZkCache().getZooKeeper(), ELECTION_ROOT,
                        jsonMapper.writeValueAsBytes(leaderBroker), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

                // Update the current leader and set the flag to true
                currentLeader.set(new LeaderBroker(leaderBroker.getServiceUrl()));
                isLeader.set(true);

                // Notify the listener that this broker is now the leader so that it can collect usage and start load
                // manager.
                log.info("Broker [{}] is the leader now, notifying the listener...", pulsar.getSafeWebServiceAddress());
                leaderListener.brokerIsTheLeaderNow();
            } catch (NodeExistsException nee) {
                // Re-elect the new leader
                log.warn(
                        "Got exception [{}] while creating election node because it already exists. Attempting re-election...",
                        nee.getMessage());
                executor.execute(this::elect);
            } catch (Exception e) {
                // Kill the broker because this broker's session with zookeeper might be stale. Killing the broker will
                // make sure that we get the fresh zookeeper session.
                log.error("Got exception [{}] while creating the election node", e.getMessage());
                pulsar.getShutdownService().shutdown(-1);
            }

        } catch (Exception e) {
            // Kill the broker
            log.error("Could not get the content of [{}], got exception [{}]. Shutting down the broker...",
                    ELECTION_ROOT, e);
            pulsar.getShutdownService().shutdown(-1);
        }
    }

    public void start() {
        checkState(stopped);
        stopped = false;
        log.info("LeaderElectionService started");
        elect();
    }

    public void stop() {
        if (stopped) {
            return;
        }

        if (isLeader()) {
            // Make sure to remove the leader election z-node in case the session doesn't
            // get closed properly. This is to avoid having to wait the session timeout
            // to elect a new one.
            // This delete operation is safe to do here (with version=-1) because either:
            //  1. The ZK session is still valid, in which case this broker is still
            //     the "leader" and we have to remove the z-node
            //  2. The session has already expired, in which case this delete operation
            //     will not go through
            try {
                pulsar.getLocalZkCache().getZooKeeper().delete(ELECTION_ROOT, -1);
            } catch (Throwable t) {
                log.warn("Failed to cleanup election root znode", t);
            }
        }
        stopped = true;
        log.info("LeaderElectionService stopped");
    }

    public LeaderBroker getCurrentLeader() {
        return currentLeader.get();
    }

    public boolean isLeader() {
        return isLeader.get();
    }

}
