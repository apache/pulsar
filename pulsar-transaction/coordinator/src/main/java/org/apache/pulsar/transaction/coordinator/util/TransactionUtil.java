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
package org.apache.pulsar.transaction.coordinator.util;

import static org.apache.pulsar.transaction.coordinator.proto.TxnStatus.ABORTED;
import static org.apache.pulsar.transaction.coordinator.proto.TxnStatus.ABORTING;
import static org.apache.pulsar.transaction.coordinator.proto.TxnStatus.COMMITTED;
import static org.apache.pulsar.transaction.coordinator.proto.TxnStatus.COMMITTING;
import org.apache.pulsar.transaction.coordinator.proto.TxnStatus;

/**
 * An transaction util of {@link TransactionUtil}.
 */
public class TransactionUtil {

    /**
     * Check if the a status can be transaction to a new status.
     *
     * @param newStatus the new status
     * @return true if the current status can be transitioning to.
     */
    public static boolean canTransitionTo(TxnStatus currentStatus, TxnStatus newStatus) {

        switch (currentStatus) {
            case OPEN:
                return newStatus != COMMITTED && newStatus != ABORTED;
            case COMMITTING:
                return newStatus == COMMITTING || newStatus == COMMITTED;
            case COMMITTED:
                return newStatus == COMMITTED;
            case ABORTING:
                return newStatus == ABORTING || newStatus == ABORTED;
            case ABORTED:
                return newStatus == ABORTED;
            default:
                throw new IllegalArgumentException("Unknown txn status : " + newStatus);
        }
    }

}
