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

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.apache.pulsar.metadata.api.MetadataSerde;

/**
 * Interface for the coordination service. Provides abstraction for distributed locks and leader election.
 */
public interface CoordinationService extends AutoCloseable {

    /**
     * Get the {@link LeaderElection} controller of a path.
     *
     * <p>There is one per path: a later call for the same path returns the same controller. If it was
     * closed, it passes its state changes, once reopened with {@link LeaderElection#elect(Object)}, to the
     * listener of the latest call. A closed controller takes no part in elections until reopened.
     *
     * @param clazz
     *            the class type to be used for serialization/deserialization
     * @param path
     *            the path to use for the leader election
     * @param stateChangesListener
     *            a listener that will be passed all the state changes
     * @return
     */
    <T> LeaderElection<T> getLeaderElection(Class<T> clazz, String path,
            Consumer<LeaderElectionState> stateChangesListener);

    <T> LockManager<T> getLockManager(Class<T> clazz);
    <T> LockManager<T> getLockManager(MetadataSerde<T> serde);

    /**
     * Increment a counter identified by the specified path and return the current value.
     *
     * The counter value will be guaranteed to be unique within the context of the path.
     * It will retry when {@link org.apache.pulsar.metadata.api.MetadataStoreException} happened.
     *
     * If the maximum number of retries is reached and still failed,
     * the feature will complete with exception {@link org.apache.pulsar.metadata.api.MetadataStoreException}.
     * @param path
     *            the path that identifies a particular counter
     * @return a future that will track the completion of the operation
     * @throws CoordinationServiceException
     *             if there's a failure in incrementing the counter
     */
    CompletableFuture<Long> getNextCounterValue(String path);

}