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
package org.apache.pulsar.client.admin;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.common.policies.data.TransactionCoordinatorStatus;

public interface Transactions {

    /**
     * Get transaction metadataStore status.
     *
     * @param coordinatorId the id which get transaction coordinator
     * @return the list future of transaction metadata store status.
     */
    CompletableFuture<TransactionCoordinatorStatus> getCoordinatorStatusById(int coordinatorId);

    /**
     * Get transaction metadataStore status.
     *
     * @return the list future of transaction metadata store status.
     */
    CompletableFuture<List<TransactionCoordinatorStatus>> getCoordinatorStatusList();

}
