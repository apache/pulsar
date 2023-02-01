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
package org.apache.bookkeeper.mledger;

import javax.annotation.Nonnull;
import org.apache.bookkeeper.common.annotation.InterfaceAudience;
import org.apache.bookkeeper.common.annotation.InterfaceStability;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.MetadataStoreException;

@InterfaceAudience.LimitedPrivate
@InterfaceStability.Stable
@SuppressWarnings({ "serial", "checkstyle:javadoctype" })
public class ManagedLedgerException extends Exception {
    public ManagedLedgerException(String msg) {
        super(msg);
    }

    protected ManagedLedgerException(Throwable e) {
        super(e);
    }

    public ManagedLedgerException(String msg, Throwable e) {
        super(msg, e);
    }

    /**
     * Returns the encoding of the given (non-null) exception as a wrapped ManagedLedgerException
     * unless it is one already. Since we use a lot of CompletableFuture, it will unwrap the completion
     * exception to help get to the original exception.
     */
    public static ManagedLedgerException getManagedLedgerException(@Nonnull Throwable ex) {
        final Throwable rc = FutureUtil.unwrapCompletionException(ex);
        if (rc instanceof ManagedLedgerException) {
            return (ManagedLedgerException) rc;
        }
        return new ManagedLedgerException(rc);
    }

    public static class MetaStoreException extends ManagedLedgerException {
        protected MetaStoreException(Throwable t) {
            super(t);
        }

        public MetaStoreException(String msg) {
            super(msg);
        }

        /**
         * Returns the encoding of the given (non-null) exception as a wrapped MetaStoreException
         * unless it is one already. Since we use a lot of CompletableFuture, it will unwrap the completion
         * exception to help get to the original exception.
         */
        public static MetaStoreException getException(Throwable ex) {
            final Throwable rc = FutureUtil.unwrapCompletionException(ex);
            if (rc instanceof MetadataStoreException.BadVersionException
                    || rc.getCause() instanceof MetadataStoreException.BadVersionException) {
                return new ManagedLedgerException.BadVersionException(ex.getMessage());
            } else if (rc instanceof MetaStoreException) {
                return (MetaStoreException) rc;
            }
            return new MetaStoreException(rc);
        }
    }

    public static class BadVersionException extends MetaStoreException {

        public BadVersionException(String msg) {
            super(msg);
        }
    }

    public static class MetadataNotFoundException extends MetaStoreException {
        public MetadataNotFoundException(String msg) {
            super(msg);
        }
    }


    public static class ManagedLedgerFencedException extends ManagedLedgerException {
        public ManagedLedgerFencedException() {
            super(new Exception("Attempted to use a fenced managed ledger"));
        }

        public ManagedLedgerFencedException(String message) {
            super(message);
        }

        public ManagedLedgerFencedException(Exception e) {
            super(e);
        }
    }

    public static class ManagedLedgerNotFoundException extends ManagedLedgerException {
        public ManagedLedgerNotFoundException(Exception e) {
            super(e);
        }

        public ManagedLedgerNotFoundException(String message) {
            super(message);
        }
    }

    public static class ManagedLedgerTerminatedException extends ManagedLedgerException {
        public ManagedLedgerTerminatedException(String msg) {
            super(msg);
        }
    }

    public static class NoMoreEntriesToReadException extends ManagedLedgerException {
        public NoMoreEntriesToReadException(String msg) {
            super(msg);
        }
    }

    public static class InvalidCursorPositionException extends ManagedLedgerException {
        public InvalidCursorPositionException(String msg) {
            super(msg);
        }
    }

    public static class ConcurrentFindCursorPositionException extends ManagedLedgerException {
        public ConcurrentFindCursorPositionException(String msg) {
            super(msg);
        }
    }

    public static class ManagedLedgerAlreadyClosedException extends ManagedLedgerException {
        public ManagedLedgerAlreadyClosedException(String msg) {
            super(msg);
        }
    }

    public static class CursorAlreadyClosedException extends ManagedLedgerException {
        public CursorAlreadyClosedException(String msg) {
            super(msg);
        }
    }

    public static class TooManyRequestsException extends ManagedLedgerException {
        public TooManyRequestsException(String msg) {
            super(msg);
        }
    }

    public static class NonRecoverableLedgerException extends ManagedLedgerException {
        public NonRecoverableLedgerException(String msg) {
            super(msg);
        }
    }

    public static class LedgerNotExistException extends NonRecoverableLedgerException {
        public LedgerNotExistException(String msg) {
            super(msg);
        }
    }

    public static class InvalidReplayPositionException extends ManagedLedgerException {
        public InvalidReplayPositionException(String msg) {
            super(msg);
        }
    }

    public static class OffloadInProgressException extends ManagedLedgerException {
        public OffloadInProgressException(String msg) {
            super(msg);
        }
    }

    public static class CursorNotFoundException extends ManagedLedgerException {
        public CursorNotFoundException(String msg) {
            super(msg);
        }
    }

    public static class ManagedLedgerInterceptException extends ManagedLedgerException {
        public ManagedLedgerInterceptException(String msg) {
            super(msg);
        }

        public ManagedLedgerInterceptException(Throwable e) {
            super(e);
        }
    }

    public static class ManagedLedgerFactoryClosedException extends ManagedLedgerException {

        public ManagedLedgerFactoryClosedException() {
            super("ManagedLedgerFactory is already closed.");
        }

    }

    public static class ConcurrentWaitCallbackException extends ManagedLedgerException {

        public ConcurrentWaitCallbackException() {
            super("We can only have a single waiting callback");
        }
    }

    public static class OffloadReadHandleClosedException extends ManagedLedgerException {

        public OffloadReadHandleClosedException() {
            super("Offload read handle already closed");
        }
    }

    @Override
    public synchronized Throwable fillInStackTrace() {
        // Disable stack traces to be filled in
        return null;
    }
}
