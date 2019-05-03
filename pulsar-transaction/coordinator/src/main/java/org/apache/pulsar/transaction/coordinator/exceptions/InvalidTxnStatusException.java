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

import org.apache.pulsar.transaction.impl.common.TxnID;
import org.apache.pulsar.transaction.impl.common.TxnStatus;

/**
 * Exception is thrown when transaction is not in the right status.
 */
public class InvalidTxnStatusException extends CoordinatorException {

    private static final long serialVersionUID = 0L;

    public InvalidTxnStatusException(String message) {
        super(message);
    }

    public InvalidTxnStatusException(TxnID txnID,
                                     TxnStatus expectedStatus,
                                     TxnStatus actualStatus) {
        super(
            "Expect Txn `" + txnID + "` to be in " + expectedStatus
                + " status but it is in " + actualStatus + " status");

    }

}
