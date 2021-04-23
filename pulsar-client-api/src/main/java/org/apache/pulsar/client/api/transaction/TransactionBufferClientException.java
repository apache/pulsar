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
import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;

/**
 * Exceptions for transaction buffer client.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class TransactionBufferClientException extends IOException {

    public TransactionBufferClientException(Throwable t) {
        super(t);
    }

    public TransactionBufferClientException(String message) {
        super(message);
    }

    /**
     * Thrown when operation timeout.
     */
    public static class RequestTimeoutException extends TransactionBufferClientException {

        public RequestTimeoutException() {
            super("Transaction buffer request timeout.");
        }

        public RequestTimeoutException(String message) {
            super(message);
        }
    }

    /**
     * Thrown when transaction buffer op over max pending numbers.
     */
    public static class ReachMaxPendingOpsException extends TransactionBufferClientException {

        public ReachMaxPendingOpsException() {
            super("Transaction buffer op reach max pending numbers.");
        }

        public ReachMaxPendingOpsException(String message) {
            super(message);
        }
    }

    public static TransactionBufferClientException unwrap(Throwable t) {
        if (t instanceof TransactionBufferClientException) {
            return (TransactionBufferClientException) t;
        } else if (t instanceof RuntimeException) {
            throw (RuntimeException) t;
        } else if (t instanceof InterruptedException) {
            Thread.currentThread().interrupt();
            return new TransactionBufferClientException(t);
        }  else if (!(t instanceof ExecutionException)) {
            // Generic exception
            return new TransactionBufferClientException(t);
        }

        Throwable cause = t.getCause();
        String msg = cause.getMessage();

        if (cause instanceof TransactionBufferClientException.RequestTimeoutException) {
            return new TransactionBufferClientException.RequestTimeoutException(msg);
        } else {
            return new TransactionBufferClientException(t);
        }

    }
}
