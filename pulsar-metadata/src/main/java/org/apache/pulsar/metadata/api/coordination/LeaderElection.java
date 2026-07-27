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
package org.apache.pulsar.metadata.api.coordination;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Leader election controller.
 */
public interface LeaderElection<T> extends AutoCloseable {
    /**
     * Try to become the leader.
     * <p>
     * Warning: because of the distributed nature of the leader election, having been promoted to "leader" status will
     * never provide a strong guarantee that no one else also thinks it's the leader. The caller will have to deal with
     * these race conditions when using the resource itself (eg. using compareAndSet() or fencing mechanisms).
     *
     * @param proposedValue
     *            the value to set for the leader, in the case this instance succeeds in becoming leader
     * @return a future that will track the completion of the operation
     *             if there's a failure in the leader election
     */
    CompletableFuture<LeaderElectionState> elect(T proposedValue);

    /**
     * Get the current leader election state.
     */
    LeaderElectionState getState();

    /**
     * Get the value set by the elected leader.
     * <p>
     * This is the authoritative read: if a leader election is currently in progress (e.g. the
     * previous leader's node was just deleted and the participants are re-electing), the returned
     * future completes once the election has settled, with the newly determined leader value. The
     * future completes exceptionally with a {@link java.util.concurrent.TimeoutException} if the
     * election does not complete within the default metadata operation timeout.
     * <p>
     * An instance that never participated in the election (no {@link #elect(Object)} call) reads
     * the leader value directly from the metadata store. A closed instance does not wait: it
     * reports an empty leader if it held the leadership when closed, or its last known view
     * otherwise.
     *
     * @return a future that will track the completion of the operation
     */
    CompletableFuture<Optional<T>> getLeaderValue();

    /**
     * Get a non-blocking snapshot of the value set by the elected leader, or empty if no leader is
     * known right now.
     * <p>
     * The snapshot can return <code>Optional.empty()</code> even though a leader is technically
     * elected (for example while a re-election is still settling). Callers that need the
     * authoritative leader must use {@link #getLeaderValue()} instead.
     */
    Optional<T> getLeaderValueIfPresent();

    /**
     * Close the leader election controller and release the leadership (if it was acquired).
     *
     * @return a future that will track the completion of the operation
     */
    CompletableFuture<Void> asyncClose();
}
