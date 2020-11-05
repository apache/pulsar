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
package org.apache.pulsar.packages.manager.exceptions;

/**
 * Base type of exception thrown by package manager.
 */
public class PackageManagerException extends Exception {
    /**
     * Constructs an {@code PackageManagerException} with the specified detail message.
     *
     * @param message
     *          The detail message (which is saved for later retrieval by the {@link #getMessage()} method)
     */
    public PackageManagerException(String message) {
        super(message);
    }

    /**
     * Constructs an {@code PackageManagerException} with the specified detail message and cause.
     *
     * @param message
     *          The detail message (which is saved for later retrieval by the {@link #getMessage()} method)
     *
     * @param throwable
     *          The cause (which is saved for later retrieval by the {@link #getCause()} method)
     */
    public PackageManagerException(String message, Throwable throwable) {
        super(message, throwable);
    }

    /**
     * Constructs an {@code PackageManagerException} with the specified cause.
     *
     * @param throwable
     *          The cause (which is saved for later retrieval by the {@link #getCause()} method)
     */
    public PackageManagerException(Throwable throwable) {
        super(throwable);
    }

    /**
     * Serialization exception thrown by metadata serialization operation.
     */
    public static class MetadataSerializationException extends PackageManagerException {
        /**
         * Constructs an {@code MetadataSerializationException} with the specified detail message.
         *
         * @param message
         *          The detail message (which is saved for later retrieval by the {@link #getMessage()} method)
         */
        public MetadataSerializationException(String message) {
            super(message);
        }

        /**
         * Constructs an {@code MetadataSerializationException} with the specified cause.
         *
         * @param throwable
         *         The cause (which is saved for later retrieval by the {@link #getCause()} method)
         */
        public MetadataSerializationException(Throwable throwable) {
            super(throwable);
        }
    }

    /**
     * Metadata not found exception thrown by getting or updating metadata operation.
     */
    public static class MetadataNotFoundException extends PackageManagerException {
        /**
         * Constructs an {@code MetadataNotFoundException} with the specified detail message.
         *
         * @param message
         *          The detail message (which is saved for later retrieval by the {@link #getMessage()} method)
         */
        public MetadataNotFoundException(String message) {
            super(message);
        }
    }

    /**
     * Package not found exception thrown by downloading a package.
     */
    public static class PackageNotFoundException extends PackageManagerException {
        /**
         * Constructs an {@code PackageNotFoundException} with the specified detail message.
         *
         * @param message
         *          The detail message (which is saved for later retrieval by the {@link #getMessage()} method)
         */
        public PackageNotFoundException(String message) {
            super(message);
        }
    }

    /**
     * Storage exception thrown by storage providers.
     */
    public static class StorageException extends PackageManagerException {
        /**
         * Constructs an {@code StorageException} with the specified detail message.
         *
         * @param message
         *          The detail message (which is saved for later retrieval by the {@link #getMessage()} method)
         */
        public StorageException(String message) {
            super(message);
        }

        /**
         * Constructs an {@code StorageException} with the specified detail message and cause.
         *
         * @param message
         *          The detail message (which is saved for later retrieval by the {@link #getMessage()} method)
         *
         * @param throwable
         *          The cause (which is saved for later retrieval by the {@link #getCause()} method)
         */
        public StorageException(String message, Throwable throwable) {
            super(message, throwable);
        }
    }
}
