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
package org.apache.pulsar.client.api.transaction;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

/**
 * Exceptions for transaction meta store client.
 */
public class TransactionMetaStoreClientException extends IOException {

    public TransactionMetaStoreClientException(Throwable t) {
        super(t);
    }

    public TransactionMetaStoreClientException(String message) {
        super(message);
    }

    /**
     * Transaction meta store client start exception.
     */
    public static final class TransactionMetaStoreClientStateException extends TransactionMetaStoreClientException {

        public TransactionMetaStoreClientStateException() {
            super("Unexpected state for transaction metadata client.");
        }

        public TransactionMetaStoreClientStateException(String message) {
            super(message);
        }
    }

    public static TransactionMetaStoreClientException unwrap(Throwable t) {
        if (t instanceof TransactionMetaStoreClientException) {
            return (TransactionMetaStoreClientException)t;
        } else if (t instanceof RuntimeException) {
            throw (RuntimeException) t;
        } else if (t instanceof InterruptedException) {
            Thread.currentThread().interrupt();
            return new TransactionMetaStoreClientException(t);
        } else {
            return new TransactionMetaStoreClientException(t);
        }
    }
}
