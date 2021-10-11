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
package org.apache.pulsar.metadata.api;

import java.io.IOException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

/**
 * Generic metadata store exception.
 */
public class MetadataStoreException extends IOException {

    public MetadataStoreException(Throwable t) {
        super(t);
    }

    public MetadataStoreException(String msg) {
        super(msg);
    }

    public MetadataStoreException(String msg, Throwable t) {
        super(msg, t);
    }

    /**
     * Implementation is invalid
     */
    public static class InvalidImplementationException extends MetadataStoreException {
        public InvalidImplementationException() {
            super((Throwable) null);
        }

        public InvalidImplementationException(Throwable t) {
            super(t);
        }

        public InvalidImplementationException(String msg) {
            super(msg);
        }
    }

    /**
     * Key not found in store.
     */
    public static class NotFoundException extends MetadataStoreException {
        public NotFoundException() {
            super((Throwable) null);
        }

        public NotFoundException(Throwable t) {
            super(t);
        }

        public NotFoundException(String msg) {
            super(msg);
        }
    }

    /**
     * Key was already in store.
     */
    public static class AlreadyExistsException extends MetadataStoreException {
        public AlreadyExistsException(Throwable t) {
            super(t);
        }

        public AlreadyExistsException(String msg) {
            super(msg);
        }
    }

    /**
     * Unsuccessful update due to mismatched expected version.
     */
    public static class BadVersionException extends MetadataStoreException {
        public BadVersionException(Throwable t) {
            super(t);
        }

        public BadVersionException(String msg) {
            super(msg);
        }
    }

    /**
     * Failed to de-serialize the metadata.
     */
    public static class ContentDeserializationException extends MetadataStoreException {
        public ContentDeserializationException(String msg, Throwable t) {
            super(msg, t);
        }

        public ContentDeserializationException(Throwable t) {
            super(t);
        }

        public ContentDeserializationException(String msg) {
            super(msg);
        }
    }

    /**
     * A resource lock is already taken by a different instance.
     */
    public static class LockBusyException extends MetadataStoreException {
        public LockBusyException() {
            super((Throwable) null);
        }

        public LockBusyException(Throwable t) {
            super(t);
        }

        public LockBusyException(String msg) {
            super(msg);
        }
    }

    /**
     * The store was already closed.
     */
    public static class AlreadyClosedException extends MetadataStoreException {
        public AlreadyClosedException(Throwable t) {
            super(t);
        }

        public AlreadyClosedException(String msg) {
            super(msg);
        }
    }

    public static MetadataStoreException unwrap(Throwable t) {
        if (t instanceof MetadataStoreException) {
            return (MetadataStoreException) t;
        } else if (t instanceof RuntimeException) {
            throw (RuntimeException) t;
        } else if (t instanceof InterruptedException) {
            return new MetadataStoreException(t);
        } else if (!(t instanceof ExecutionException) && !(t instanceof CompletionException)) {
            // Generic exception
            return new MetadataStoreException(t);
        }

        // Unwrap the exception to keep the same exception type but a stack trace that includes the application calling
        // site
        Throwable cause = t.getCause();
        String msg = cause.getMessage();
        if (cause instanceof NotFoundException) {
            return new NotFoundException(msg);
        } else if (cause instanceof AlreadyExistsException) {
            return new AlreadyExistsException(msg);
        } else if (cause instanceof BadVersionException) {
            return new BadVersionException(msg);
        } else if (cause instanceof ContentDeserializationException) {
            return new ContentDeserializationException(msg);
        } else if (cause instanceof InvalidImplementationException) {
            return new InvalidImplementationException(msg);
        } else if (cause instanceof LockBusyException) {
            return new LockBusyException(msg);
        } else {
            return new MetadataStoreException(t);
        }
    }
}
