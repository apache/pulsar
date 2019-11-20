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
package org.apache.pulsar.transaction.coordinator.exceptions;

import org.apache.pulsar.transaction.coordinator.TransactionMetadataStore;

/**
 * Exception is thrown when update the state incorrect in transaction store.
 */
public class TxnStoreStateUpdateException extends CoordinatorException {

    private static final long serialVersionUID = 0L;

    public TxnStoreStateUpdateException(String message) {
        super(message);
    }

    public TxnStoreStateUpdateException(String tcID,
                                        TransactionMetadataStore.State oldState,
                                        TransactionMetadataStore.State newState) {
        super(
                "Expect tcID `" + tcID + "` to be in " + oldState
                        + " status but it is in " + newState + " status");

    }
}
