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
     * Get the value set by the elected leader, or empty if there's currently no leader.
     *
     * @return a future that will track the completion of the operation
     */
    CompletableFuture<Optional<T>> getLeaderValue();

    /**
     * Get the value set by the elected leader, or empty if there's currently no leader.
     * <p>
     * The call is non blocking and in certain cases can return <code>Optional.empty()</code> even though a leader is
     * technically elected.
     *
     * @return a future that will track the completion of the operation
     */
    Optional<T> getLeaderValueIfPresent();

    /**
     * Close the leader election controller and release the leadership (if it was acquired).
     *
     * @return a future that will track the completion of the operation
     */
    CompletableFuture<Void> asyncClose();
}
